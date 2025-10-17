from typing import List, Optional, Tuple
import logging
import psycopg2.extras

from tables_ops import (
    ensure_clone_table,
    fetch_table_columns,
    record_exists_by_id,
    insert_from_old_by_id,
)
from extract_helpers import extract_filename, infer_customer_type, extract_company, extract_course_language
from taas_schools import detect_taas_school

PROGRESS_EVERY = 100  # Log progress every N paths

def find_courses_by_spreadsheet_name(conn, spreadsheet_name: str) -> List[dict]:
    """Find rows in legacy course table by exact spreadsheet_name.

    Exact, case-sensitive, accent-sensitive match; no normalization.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM public.course_old WHERE spreadsheet_name = %s",
            (spreadsheet_name,),
        )
        return list(cur.fetchall())


def find_new_course_by_spreadsheet_name(conn, spreadsheet_name: str) -> List[dict]:
    """Find rows in new_course by exact spreadsheet_name."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM public.new_course WHERE spreadsheet_name = %s",
            (spreadsheet_name,),
        )
        return list(cur.fetchall())


def update_student_is_2on1(conn, student_id: Optional[int], is_2on1: bool, dry_run: bool = False) -> None:
    if student_id is None:
        return
    if dry_run:
        logging.debug(
            f"[dry-run] Would update new_student_data id={student_id} set is_2on1=%r",
            is_2on1,
        )
        return
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE public.new_student_data SET is_2on1 = %s WHERE id = %s",
            (is_2on1, student_id),
        )
    logging.info(f"Updated new_student_data id={student_id} is_2on1={is_2on1}")


def update_new_course(conn, row_id: int, type_value: str, company_name: str, course_language: str, taas_school: str, dry_run: bool = False) -> None:
    """Update a new_course row with inferred fields.

    Only sets optional fields (course_language, taas_school) if columns exist.
    """
    # Build dynamic SET list based on existing columns to avoid errors if columns are missing
    cols = fetch_table_columns(conn, 'new_course')
    # Normalize to uppercase for DB storage; use None for empty strings
    type_db = (type_value or '').upper() or None
    company_db = (company_name or '').upper() or None
    lang_db = (course_language or '').upper() or None
    taas_school_db = (taas_school or '').upper() or None

    set_parts = ["type = %s", "company_name = %s"]
    params = [type_db, company_db]
    if 'course_language' in cols:
        set_parts.append("course_language = %s")
        params.append(lang_db)
    if 'taas_school' in cols:
        set_parts.append("taas_school = %s")
        params.append(taas_school_db if type_db == 'TAAS' else None)

    set_sql = ", ".join(set_parts)
    if dry_run:
        # Use logging parameter substitution and keep this at DEBUG to avoid clutter
        logging.debug(
            "[dry-run] Would update new_course id=%s set %s with params=%r",
            row_id,
            set_sql,
            params,
        )
        return
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE public.new_course SET {set_sql} WHERE id = %s",
            (*params, row_id),
        )
    logging.info(
        f"Updated new_course id={row_id} type={type_db} company_name={company_db!r} course_language={lang_db!r} taas_school={taas_school_db!r}"
    )


def find_classes_by_course_id(conn, course_id) -> List[dict]:
    """Fetch related classes from legacy table by course_id."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM public.class_old WHERE course_id = %s", (course_id,))
        return list(cur.fetchall())


def copy_student_if_needed(conn, student_id: Optional[int], student_cols: List[str], dry_run: bool = False) -> bool:
    """Ensure a student exists in student_taas by copying from student_data_old.

    Returns True if a copy would happen (or did happen), else False.
    """
    if student_id is None:
        return False
    ensure_clone_table(conn, 'student_data_old', 'student_taas')
    if record_exists_by_id(conn, 'student_taas', student_id):
        return False
    if dry_run:
        logging.info(f"[dry-run] Would copy student_data_old id={student_id} -> student_taas")
        return True
    insert_from_old_by_id(conn, 'student_data_old', 'student_taas', student_cols, student_id)
    logging.info(f"Copied student_data_old id={student_id} -> student_taas")
    return True


def copy_course_and_related(
    conn,
    course_row: dict,
    customer_type: Optional[str],
    course_cols: List[str],
    class_cols: List[str],
    student_cols: List[str],
    dry_run: bool = False,
) -> Tuple[bool, int, int, int]:
    """Copy a course and its classes/students into *_taas clones when applicable."""
    course_id = course_row['id']
    student_id = course_row.get('student_id')

    # If no customer type (neither TAAS nor B2B), skip copying entirely
    if not customer_type:
        logging.debug(f"Skip copy for course id={course_id}: no customer_type inferred")
        return False, 0, 0, course_id

    ensure_clone_table(conn, 'course_old', 'course_taas')
    ensure_clone_table(conn, 'class_old', 'class_taas')

    course_copied = False
    if not record_exists_by_id(conn, 'course_taas', course_id):
        if dry_run:
            logging.info(f"[dry-run] Would copy course_old id={course_id} -> course_taas")
            course_copied = True
        else:
            insert_from_old_by_id(conn, 'course_old', 'course_taas', course_cols, course_id)
            course_copied = True
            logging.info(f"Copied course_old id={course_id} -> course_taas")

    if customer_type and not dry_run:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE public.course_taas SET customer_type = %s WHERE id = %s",
                (customer_type, course_id),
            )
            logging.info(f"Set course_taas.id={course_id} customer_type={customer_type}")

    classes = find_classes_by_course_id(conn, course_id)
    classes_copied = 0
    for cls in classes:
        cls_id = cls['id']
        if not record_exists_by_id(conn, 'class_taas', cls_id):
            if dry_run:
                logging.info(f"[dry-run] Would copy class_old id={cls_id} -> class_taas")
                classes_copied += 1
            else:
                insert_from_old_by_id(conn, 'class_old', 'class_taas', class_cols, cls_id)
                classes_copied += 1
                logging.info(f"Copied class_old id={cls_id} -> class_taas")

    student_copied = 0
    if copy_student_if_needed(conn, student_id, student_cols, dry_run=dry_run):
        student_copied = 1

    if not dry_run:
        conn.commit()

    return course_copied, classes_copied, student_copied, course_id


def _read_input_lines(input_path: str) -> List[str]:
    """Read file robustly trying common encodings to preserve diacritics."""
    # Read bytes and try common encodings to preserve diacritics
    with open(input_path, 'rb') as f:
        data = f.read()
    for enc in ("utf-8-sig", "utf-8"):
        try:
            text = data.decode(enc)
            if enc != "utf-8":
                logging.info("Decoded input using encoding=%s", enc)
            return text.splitlines()
        except UnicodeDecodeError:
            pass
    for enc in ("cp1252", "latin-1"):
        try:
            text = data.decode(enc)
            logging.info("Decoded input using fallback encoding=%s", enc)
            return text.splitlines()
        except UnicodeDecodeError:
            pass
    # Last resort: replace undecodable bytes
    text = data.decode("utf-8", errors="replace")
    logging.warning("Decoded input with replacement characters; some matches may fail")
    return text.splitlines()


def orchestrate(conn, input_path: str, dry_run: bool = False):
    """Main pipeline: read paths, infer fields, and update DB rows."""
    total_paths = 0
    total_updates = 0
    total_matched_rows = 0

    logging.info(f"Reading input file: {input_path} (dry_run={dry_run})")
    for line in _read_input_lines(input_path):
        s = line.strip()
        if not s:
            continue
        total_paths += 1

        filename = extract_filename(s)
        if not filename:
            logging.debug(f"Skip unparsable line: {s}")
            continue

        # Infer type from the path; default to b2c if none
        inferred = infer_customer_type(s)
        if inferred is None:
            type_value = 'b2c'
        else:
            # Map previous labels to lowercase for new schema
            type_value = inferred.lower()  # 'TAAS'/'B2B' -> 'taas'/'b2b'

        company_name = extract_company(s)
        course_language = extract_course_language(s)
        taas_school = detect_taas_school(s) if (type_value == 'taas') else None
        is_2on1 = ('2-1' in s)

        rows = find_new_course_by_spreadsheet_name(conn, filename)
        if rows:
            total_matched_rows += len(rows)
            # Print a concise, readable block per path
            logging.info("* %s | Match", s)
            for row in rows:
                new_type = (type_value or '').upper()
                new_company = (company_name or '').upper()
                new_lang = (course_language or '').upper()
                new_taas_school = (taas_school or '').upper() if new_type == 'TAAS' else ''

                logging.info(
                    "new_course: [type:%s, company_name:%s, course_language:%s, taas_school:%s]",
                    new_type, new_company or '', new_lang or '', new_taas_school or ''
                )
                logging.info("new_student_data: [is_2on1:%s]", is_2on1)

                update_new_course(conn, row['id'], type_value, company_name, course_language, taas_school, dry_run=dry_run)
                update_student_is_2on1(conn, row.get('student_id'), is_2on1, dry_run=dry_run)
                total_updates += 1
        else:
            logging.info("* %s | No Match", s)

    if not dry_run:
        conn.commit()

    return {
        'paths_processed': total_paths,
        'matched_rows': total_matched_rows,
        'rows_updated': total_updates,
    }

#!/usr/bin/env python3
import argparse
import logging

from dotenv import load_dotenv
from psycopg2 import sql

from db_conn import get_conn
from tables_ops import ensure_clone_table, fetch_table_columns


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s %(levelname)s %(message)s')


def table_exists(conn, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table,),
        )
        return cur.fetchone() is not None


def drop_table(conn, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS public.{t}").format(t=sql.Identifier(table)))
    conn.commit()


def insert_all_from_source(
    conn,
    source_table: str,
    target_table: str,
    source_alias: str = "s",
) -> int:
    """Insert all rows (by matching column list) from source_table into target_table.

    Uses OVERRIDING SYSTEM VALUE to allow explicit IDs when target has identity columns.
    Returns the number of rows inserted (as reported by cursor.rowcount; may be -1 in some cases).
    """
    cols = fetch_table_columns(conn, source_table)
    if not cols:
        return 0
    cols_ident = sql.SQL(',').join(sql.Identifier(c) for c in cols)
    src_exprs = sql.SQL(',').join(sql.SQL(f"{source_alias}.{{}}" ).format(sql.Identifier(c)) for c in cols)
    q = sql.SQL(
        "INSERT INTO public.{target} ({cols}) OVERRIDING SYSTEM VALUE "
        "SELECT {src_exprs} FROM public.{source} {alias}"
    ).format(
        target=sql.Identifier(target_table),
        cols=cols_ident,
        src_exprs=src_exprs,
        source=sql.Identifier(source_table),
        alias=sql.SQL(source_alias),
    )
    with conn.cursor() as cur:
        cur.execute(q)
        try:
            return cur.rowcount or 0
        except Exception:
            return 0


def insert_missing_from_taas(
    conn,
    taas_table: str,
    base_table: str,
    target_table: str,
    updated_at_column: str = "updated_at",
) -> int:
    """Insert rows present in taas_table but not in base_table (by id) into target_table.

    - Only columns present in both taas_table and base_table are inserted (intersection),
      ordered by base_table column order.
    - If updated_at_column is present in the intersection, the value for inserted rows is NOW().
    - Uses OVERRIDING SYSTEM VALUE to allow explicit IDs even if target has identity columns.
    Returns the number of rows inserted (as reported by cursor.rowcount; may be -1 in some cases).
    """
    base_cols = fetch_table_columns(conn, base_table)
    taas_cols = fetch_table_columns(conn, taas_table)
    if not base_cols or not taas_cols:
        logging.warning(
            "Skipping merge for %s -> %s (missing columns info)", taas_table, target_table
        )
        return 0

    # Ensure we can compare by id
    if "id" not in base_cols or "id" not in taas_cols:
        logging.warning(
            "Skipping merge for %s -> %s because 'id' column missing", taas_table, target_table
        )
        return 0

    # Build set of columns we will insert, in base order. We can provide
    # values for columns present in TAAS; additionally, always include
    # updated_at if it exists in the base/join table so we can set NOW().
    insertable_cols = [
        c for c in base_cols if (c in taas_cols) or (c == updated_at_column)
    ]
    if not insertable_cols:
        logging.warning(
            "No overlapping columns between %s and %s; nothing to insert",
            base_table,
            taas_table,
        )
        return 0

    target_cols_sql = sql.SQL(',').join(sql.Identifier(c) for c in insertable_cols)

    exprs = []
    for c in insertable_cols:
        if c == updated_at_column:
            exprs.append(sql.SQL("NOW()"))
        else:
            exprs.append(sql.SQL("t.{col}").format(col=sql.Identifier(c)))
    source_exprs_sql = sql.SQL(',').join(exprs)

    q = sql.SQL(
        """
        INSERT INTO public.{target} ({cols}) OVERRIDING SYSTEM VALUE
        SELECT {src_exprs}
        FROM public.{taas} t
        WHERE NOT EXISTS (
            SELECT 1 FROM public.{base} b WHERE b.id = t.id
        )
        """
    ).format(
        target=sql.Identifier(target_table),
        cols=target_cols_sql,
        src_exprs=source_exprs_sql,
        taas=sql.Identifier(taas_table),
        base=sql.Identifier(base_table),
    )
    with conn.cursor() as cur:
        cur.execute(q)
        try:
            return cur.rowcount or 0
        except Exception:
            return 0


def build_join_table(
    conn,
    base_table: str,
    taas_table: str,
    join_table: str,
    recreate: bool = True,
) -> None:
    """Create and populate the join_table with the union of base_table and taas_table.

    - If recreate is True, drops the join_table first (if exists) and clones structure from base_table.
    - Inserts all rows from base_table.
    - Inserts rows from taas_table where id does not exist in base_table. For these rows, sets updated_at to NOW() if present.
    """
    logging.info("Preparing %s from %s + %s", join_table, base_table, taas_table)

    if recreate and table_exists(conn, join_table):
        logging.debug("Dropping existing table %s", join_table)
        drop_table(conn, join_table)

    ensure_clone_table(conn, base_table, join_table)

    logging.debug("Copying all rows from %s -> %s", base_table, join_table)
    inserted_from_base = insert_all_from_source(conn, base_table, join_table, source_alias="b")
    logging.info("Inserted %s rows from %s", inserted_from_base, base_table)

    logging.debug("Adding missing rows from %s (not in %s) -> %s", taas_table, base_table, join_table)
    inserted_from_taas = insert_missing_from_taas(conn, taas_table, base_table, join_table)
    logging.info("Inserted %s new rows from %s", inserted_from_taas, taas_table)

    conn.commit()


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Build *_join tables by merging *_taas with existing tables")
    parser.add_argument(
        "--no-recreate",
        dest="recreate",
        action="store_false",
        help="Do not drop existing *_join tables before inserting",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose logging",
    )
    args = parser.parse_args()

    setup_logging(args.verbose)

    conn = get_conn()
    try:
        # Order: course, class, student_data
        build_join_table(conn, base_table="course", taas_table="course_taas", join_table="course_join", recreate=args.recreate)
        build_join_table(conn, base_table="class", taas_table="class_taas", join_table="class_join", recreate=args.recreate)
        build_join_table(conn, base_table="student_data", taas_table="student_taas", join_table="student_data_join", recreate=args.recreate)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

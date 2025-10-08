from typing import List
from psycopg2 import sql


def ensure_clone_table(conn, old_table: str, new_table: str) -> None:
    with conn.cursor() as cur:
        # Check if the target table exists
        cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (new_table,),
        )
        exists = cur.fetchone() is not None
        if not exists:
            # Create table by cloning structure from the old table
            cur.execute(
                sql.SQL(
                    "CREATE TABLE public.{new} (LIKE public.{old} INCLUDING IDENTITY INCLUDING DEFAULTS)"
                ).format(
                    new=sql.Identifier(new_table),
                    old=sql.Identifier(old_table),
                )
            )
    conn.commit()


def fetch_table_columns(conn, table: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        rows = cur.fetchall()
        return [r[0] for r in rows]


def record_exists_by_id(conn, table: str, id_value) -> bool:
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM public.{table} WHERE id = %s LIMIT 1", (id_value,))
        return cur.fetchone() is not None


def insert_from_old_by_id(conn, old_table: str, new_table: str, columns: List[str], id_value) -> None:
    cols_csv = ','.join([f'"{c}"' for c in columns])
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO public.{new_table} ({cols_csv}) SELECT {cols_csv} FROM public.{old_table} WHERE id = %s",
            (id_value,),
        )

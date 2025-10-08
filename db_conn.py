import os
import psycopg2


def get_db_url() -> str:
    url = os.getenv('DATABASE_PUBLIC_URL')
    if not url:
        raise RuntimeError('DATABASE_PUBLIC_URL is required')
    return url


def get_conn():
    return psycopg2.connect(get_db_url())


import os
import psycopg2


def get_db_url() -> str:
    """Get database URL from env var DATABASE_PUBLIC_URL."""
    url = os.getenv('DATABASE_PUBLIC_URL')
    if not url:
        raise RuntimeError('DATABASE_PUBLIC_URL is required')
    return url


def get_conn():
    """Open a new psycopg2 connection using DATABASE_PUBLIC_URL."""
    return psycopg2.connect(get_db_url())

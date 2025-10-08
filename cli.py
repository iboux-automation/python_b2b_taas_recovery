#!/usr/bin/env python3
import argparse
import logging
import os

from db_conn import get_conn
from logic_copy import orchestrate


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s %(levelname)s %(message)s')


def main():
    parser = argparse.ArgumentParser(description='Copy *_old tables to *_taas based on b2b paths match by spreadsheet_name')
    parser.add_argument('--input', default='iboux-system-be/python_b2b_recovery/b2b_paths/b2b_paths.cleaned.csv', help='Input file with one path per line')
    parser.add_argument('--dry-run', action='store_true', help='Do not write to DB, only log actions')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    args = parser.parse_args()

    setup_logging(args.verbose)

    if not os.path.exists(args.input):
        logging.warning(f"Input file not found: {args.input}")

    conn = get_conn()
    try:
        summary = orchestrate(conn, args.input, dry_run=args.dry_run)
        logging.info(
            "Done. Paths processed=%s, course matches=%s, courses copied=%s, classes copied=%s, students copied=%s",
            summary['paths_processed'], summary['course_matches'], summary['courses_copied'], summary['classes_copied'], summary['students_copied']
        )
    finally:
        conn.close()


if __name__ == '__main__':
    main()


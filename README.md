Python B2B/TaaS Recovery

Overview
- Reads each path from `b2b_paths/b2b_paths.cleaned.csv` (or a provided file).
- Extracts the filename (after the last `/` and after the last `___`).
- Finds matches in `public.course_old` where `spreadsheet_name` equals that filename.
- Copies the matching `course_old` rows to `public.course_taas`.
- Copies related `class_old` rows (by `course_id`) to `public.class_taas`.
- Copies the related `student_old` row (by `course_old.student_id`) to `public.student_taas`.
- Updates `course_taas.customer_type` based on the path text:
  - Contains `TaaS` or `Babbel` → `TAAS`
  - Else contains `B2B` or `Companies` → `B2B`
  - If both apply, `TAAS` wins
  - Matching is case-insensitive

Notes
- The script creates `course_taas`, `class_taas`, and `student_taas` tables if they do not exist, cloning the column structure from `*_old` tables.
- It avoids foreign keys and indexes in the clone to prevent cross-table dependency headaches. Column order is preserved.
- Records are inserted only if the same `id` is not already present in the target table.

Requirements
- Environment variable `DATABASE_PUBLIC_URL` (or `DATABASE_URL`) must point to your PostgreSQL instance (Railway compatible).
- Python 3 with `psycopg2` installed.

Usage
- Dry run (no DB writes):
  `python iboux-system-be/python_b2b_recovery/run_taas_copy.py --dry-run --verbose`
- Real run:
  `python iboux-system-be/python_b2b_recovery/run_taas_copy.py`
- Specify a different input file:
  `python iboux-system-be/python_b2b_recovery/run_taas_copy.py --input path/to/file.txt`

Input File
- One path per line. Example line:
  `EX-STUDENTS1/Ex-Students - TaaS___ABA English Ex-Students___ABA English - NEW___Carla Sanches (ABA ENGLISH) GB 22917764`
- Extracted filename → `Carla Sanches (ABA ENGLISH) GB 22917764`


Python B2B/TaaS Recovery

Overview
- Reads each path from `b2b_paths/b2b_paths.cleaned.csv` (or a provided file).
- Extracts the filename (after the last `/` and after the last `___`).
- Determines customer type (TAAS or B2B) from the path; if neither applies, the line is skipped and nothing is copied.
- Finds matches in `public.course_old` where `spreadsheet_name` equals that filename.
- Copies the matching `course_old` rows to `public.course_taas`.
- Copies related `class_old` rows (by `course_id`) to `public.class_taas`.
- Copies the related `student_data_old` row (by `course_old.student_id`) to `public.student_taas`.
- Updates `course_taas.customer_type` based on the path text:
  - Contains `TaaS` or `Babbel` → `TAAS`
  - Else contains `B2B` or `Companies` → `B2B`
  - If both apply, `TAAS` wins
  - Matching is case-insensitive

Notes
- The script creates `course_taas`, `class_taas`, and `student_taas` tables if they do not exist, cloning the column structure from `*_old` tables.
- It avoids foreign keys and indexes in the clone to prevent cross-table dependency headaches. Column order is preserved.
- Records are inserted only if the same `id` is not already present in the target table.
- Lines without TAAS/B2B classification are ignored; no destination tables are created nor records inserted for those lines.

Requirements
- Environment variable `DATABASE_PUBLIC_URL` must point to your PostgreSQL instance (Railway compatible).
- For local dev, put it in `.env` and it will be auto‑loaded.
- Python 3 with `psycopg2-binary` and `python-dotenv` (installed via `requirements.txt`).

Usage
- Dry run (no DB writes):
  `python run_taas_copy.py --dry-run --verbose`
- Real run:
  `python run_taas_copy.py`
- Specify a different input file:
  `python run_taas_copy.py --input path/to/file.txt`

Join Tables Builder
- Builds union tables `course_join`, `class_join`, and `student_data_join` by merging existing tables with `*_taas` tables.
- Keeps original rows from `course`, `class`, and `student_data` by `id`. Only adds rows from `*_taas` whose `id` does not exist in the original tables.
- Sets `updated_at = NOW()` for the newly added rows from `*_taas` in the join tables (if the column exists).

Run:
- Recreate join tables (drop if exist, then rebuild):
  `python join_script/run_build_joins.py --verbose`
- Keep existing join tables structure (do not drop first):
  `python join_script/run_build_joins.py --no-recreate`

Railway
- Add a new Python service and connect this repo.
- Set env var `DATABASE_PUBLIC_URL` in Railway to your Postgres URL.
- The `Procfile` runs `python -u run_taas_copy.py` as a worker.
- Optionally override the start command in the UI to add flags (e.g. `--dry-run`).

Input File
- One path per line. Example line:
  `EX-STUDENTS1/Ex-Students - TaaS___ABA English Ex-Students___ABA English - NEW___Carla Sanches (ABA ENGLISH) GB 22917764`
- Extracted filename → `Carla Sanches (ABA ENGLISH) GB 22917764`

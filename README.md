Python B2B/TaaS Recovery

Overview
- Reads each path from `b2b_paths/b2b_paths.cleaned.csv` (or a provided file).
- Extracts the filename (after the last `/` and after the last `___`).
- Infers type from the path: contains `TaaS`/`Babbel` → `taas`; contains `B2B`/`Companies` → `b2b`; otherwise `b2c`.
- Finds matches in `public.new_course` where `spreadsheet_name` equals that filename (exact match).
- Updates matched rows in `public.new_course`:
  - Sets `type` to `taas`, `b2b`, or `b2c`.
  - Sets `company_name` using the penultimate `___` segment from the path; if that segment contains the exact delimiter ` - `, uses only the substring after the last ` - ` (e.g., `"Travis - Korott"` → `"Korott"`).
  - Sets `course_language` to one of `IT`, `ES`, `EN`, `FR`, `DE` when the code appears with non-letters on both sides (or boundaries). Examples: `"[DE-"`, `" EN "`, `"_IT "`, `"(FR)"` are valid; `"aDE "` is not valid, `"a DE "` is valid.
  - Sets related `new_student_data.is_2on1` to `true` if the full path contains the exact substring `"2-1"`; otherwise sets it to `false`.
  - If `type` resolves to `taas`, sets `taas_school` (when the column exists) using a configurable mapping in `taas_schools.py` (e.g., path contains `"babbel"` → `BABBEL`, `"hola"` → `hola`). You can extend this list in that file.

Notes
- No rows are inserted; only existing rows in `public.new_course` are updated if a filename match is found.
- If a path does not imply `taas` or `b2b`, the `type` defaults to `b2c`.

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

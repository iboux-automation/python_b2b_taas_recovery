import argparse
import os
import re


def transform_path(line: str) -> str:
    s = line.strip().rstrip("\r")
    if not s:
        return ""

    if not s.startswith("gs://"):
        # Not a GCS path; return as-is
        return s

    # Remove scheme
    without_scheme = s[len("gs://"):]

    # Split into components by '/'
    parts = without_scheme.split("/")

    if len(parts) < 2:
        # Only bucket provided, nothing else to transform
        return s

    first_folder = parts[1] if len(parts) > 1 else ""

    # Last component may be empty if path ends with '/'
    leaf = parts[-1]

    if leaf == "":
        # Only folder provided under bucket
        return f"{first_folder}/" if first_folder else ""

    # If there are subfolders, ignore them and keep only the final leaf filename
    # Remove .tsv or .tsv.<anything> suffixes (case-insensitive)
    leaf_clean = re.sub(r"\.tsv(?:\..*)?$", "", leaf, flags=re.IGNORECASE)

    return f"{first_folder}/{leaf_clean}" if first_folder else leaf_clean


def main():
    parser = argparse.ArgumentParser(description="Clean GCS paths: keep first folder and filename without .tsv/.tsv.*")
    parser.add_argument("--input", default="iboux-system-be/python_b2b_recovery/b2b_paths.csv", help="Input CSV file with GCS paths")
    parser.add_argument("--output", default="iboux-system-be/python_b2b_recovery/b2b_paths.cleaned.csv", help="Output CSV path")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    with open(args.input, "r", encoding="utf-8", errors="replace") as fin, \
         open(args.output, "w", encoding="utf-8") as fout:
        for line in fin:
            cleaned = transform_path(line)
            if cleaned == "" and line.strip() == "":
                # preserve blank lines
                fout.write("\n")
            elif cleaned != "":
                fout.write(cleaned + "\n")


if __name__ == "__main__":
    main()


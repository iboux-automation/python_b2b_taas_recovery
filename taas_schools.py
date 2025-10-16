from typing import Optional

# Extendable mapping of substrings in the path -> normalized school label
# Add more entries here as needed.
TAAS_SCHOOLS = {
    "babbel": "BABBEL",
}


def detect_taas_school(path: str) -> Optional[str]:
    s = path.lower()
    for needle, label in TAAS_SCHOOLS.items():
        if needle in s:
            return label
    return None


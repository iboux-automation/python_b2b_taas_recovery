import re


def infer_customer_type(path: str):
    s = path.lower()
    is_taas = ('taas' in s) or ('babbel' in s)
    is_b2b = ('b2b' in s) or ('companies' in s)
    if is_taas:
        return 'TAAS'
    if is_b2b:
        return 'B2B'
    return None


def extract_filename(path: str) -> str:
    s = path.strip().rstrip('\r')
    if not s:
        return ''
    tail = s.rsplit('/', 1)[-1]
    tail = tail.split('___')[-1]
    tail = re.sub(r"\.tsv(?:\.(?:done|empty))?$", "", tail, flags=re.IGNORECASE)
    return tail.strip()


def extract_company(path: str) -> str:
    s = path.strip().rstrip('\r')
    if not s:
        return ''
    tail = s.rsplit('/', 1)[-1]
    parts = tail.split('___')
    # Heuristic: company segment is the penultimate '___' part, if present
    if len(parts) >= 2:
        segment = parts[-2].strip()
        # If the exact delimiter " - " exists, take the part after the last occurrence
        if " - " in segment:
            segment = segment.rsplit(" - ", 1)[-1].strip()
        return segment
    return ''


def extract_course_language(path: str) -> str:
    s = path.strip().rstrip('\r')
    if not s:
        return ''
    # Match language codes only when adjacent characters are not letters (or boundaries)
    # Examples that should match: "[DE-", " EN ", "_IT ", "(FR)", " a DE ";
    # Examples that should not match: "aDE ", "abcENz"
    m = re.search(r"(?<![A-Za-z])(IT|ES|EN|FR|DE)(?![A-Za-z])", s)
    if m:
        return m.group(1)
    return ''

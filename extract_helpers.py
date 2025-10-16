import re
from typing import Optional
from taas_schools import detect_taas_school


def infer_customer_type(path: str):
    """Infer high-level customer type from full path.

    - Returns 'TAAS' if the path contains the word 'taas' OR any TAAS school keyword
      from `taas_schools.detect_taas_school`.
    - Returns 'B2B' if the path mentions 'b2b' or 'companies'.
    - Otherwise returns None.
    All checks are case-insensitive.
    """
    s = path.lower()
    is_taas = ('taas' in s) or (detect_taas_school(path) is not None)
    is_b2b = ('b2b' in s) or ('companies' in s)
    if is_taas:
        return 'TAAS'
    if is_b2b:
        return 'B2B'
    return None


def extract_filename(path: str) -> str:
    """Extract the spreadsheet/file name from a path.

    Takes the last '/' segment, then the last '___' subsegment, and strips
    trailing extensions like .tsv(.done|.empty).
    """
    s = path.strip().rstrip('\r')
    if not s:
        return ''
    tail = s.rsplit('/', 1)[-1]
    tail = tail.split('___')[-1]
    tail = re.sub(r"\.tsv(?:\.(?:done|empty))?$", "", tail, flags=re.IGNORECASE)
    return tail.strip()


def extract_company(path: str) -> str:
    """Extract company segment from path.

    Uses the penultimate '___' segment; if it contains " - ", keep only the
    substring after the last " - ". Returns empty string if none.
    """
    s = path.strip().rstrip('\r')
    if not s:
        return ''
    tail = s.rsplit('/', 1)[-1]
    parts = tail.split('___')
    # Prefer the segment immediately after a 'Companies' marker if present.
    # Example: ".../Companies___Travis - Korott___..." -> "Travis - Korott"
    comp_idx = None
    for i, p in enumerate(parts):
        # Match segments like 'Companies' or '* - Companies' (case-insensitive)
        if p.strip().lower().endswith('companies'):
            comp_idx = i
            break
    segment = ''
    if comp_idx is not None and comp_idx + 1 < len(parts):
        segment = parts[comp_idx + 1].strip()

    if segment:
        # If the exact delimiter " - " exists, take the part after the last occurrence
        if " - " in segment:
            segment = segment.rsplit(" - ", 1)[-1].strip()
        # Return uppercased company for consistency with DB normalization
        return segment.upper()
    return ''


def extract_course_language(path: str) -> str:
    """Extract course language code (IT, ES, EN, FR, DE) from path.

    Preference order:
    1) Inside square brackets (e.g., "[DE - Babbel]")
    2) Anywhere in the path, but only when next to non-letters or edges
       (so 'aDE ' is ignored, 'a DE ' is valid).
    Returns empty string if not found.
    """
    s = path.strip().rstrip('\r')
    if not s:
        return ''
    # 1) Prefer language codes found inside square brackets, e.g. "[DE - Babbel]", "[ EN ]"
    for m in re.finditer(r"\[([^\]]+)\]", s):
        bracket_text = m.group(1)
        m_in = re.search(r"(?<![A-Za-z])(IT|ES|EN|FR|DE)(?![A-Za-z])", bracket_text)
        if m_in:
            return m_in.group(1)
    # 2) Fallback: search the whole string with boundary rules
    m = re.search(r"(?<![A-Za-z])(IT|ES|EN|FR|DE)(?![A-Za-z])", s)
    if m:
        return m.group(1)
    return ''

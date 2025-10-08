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


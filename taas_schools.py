from typing import Optional

# Extendable mapping of substrings in the path -> normalized school label
# Add more entries here as needed.
TAAS_SCHOOLS = {
    "babbel": "BABBEL",
    "eureka": "EUREKA",
    "au pays des langues": "AU PAYS DES LANGUES",
    "pro english courses": "PRO ENGLISH COURSES",
    "pronto english": "PRONTO ENGLISH",
    "intellecto": "INTELLECTO",
    "salt idiomes": "SALT IDIOMES",
    "language link": "LANGUAGE LINK",
    "lic formation": "LIC FORMATION",
    "instituto europeo de formación": "INSTITUTO EUROPEO DE FORMACIÓN",
    "international travel advisor": "INTERNATIONAL TRAVEL ADVISOR",
    "academy aziendali": "ACADEMY AZIENDALI",
    "altissa": "ALTISSA",
}


def detect_taas_school(path: str) -> Optional[str]:
    s = path.lower()
    for needle, label in TAAS_SCHOOLS.items():
        if needle in s:
            return label
    return None


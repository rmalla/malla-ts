"""
Name formatting utilities for beautifying DLA source data.

Raw DLA data is ALL CAPS with verbose legal suffixes. These functions
produce clean display names while preserving the raw data in the DB.

Manufacturer: ROCKWELL COLLINS, INC. DBA COLLINS AEROSPACE GOVERNMENT SYSTEMS
           -> Collins Aerospace

Product:      COMPRESSOR UNIT,RECIPROCATING
           -> Compressor Unit, Reciprocating
"""

import re

# Legal suffixes to strip (applied repeatedly until stable)
# Note: INTERNATIONAL is NOT here — it's a real part of company names
_LEGAL_PATTERN = re.compile(
    r'[,.]?\s*\b('
    r'INC\.?|INCORPORATED|CORP\.?|CORPORATION|LLC|LTD|L\.?L\.?C\.?|'
    r'CO\.?|COMPANY|GROUP|ENTERPRISES?|INTL'
    r')\b\.?\s*$',
    re.IGNORECASE,
)

# Legal words that appear mid-name and should be stripped along with everything after
_MID_LEGAL = re.compile(
    r'\b(INC\.?|CORP\.?|CORPORATION)\b\.?\s+(?!DBA|DIV)',
    re.IGNORECASE,
)

# Multi-word department/site suffixes to strip (must be 2+ words to avoid
# stripping legitimate single-word parts of company names)
_DEPT_SUFFIXES = re.compile(
    r'\s+('
    r'GOVERNMENT\s+SYSTEMS|DEFENSE\s+\w+|'
    r'ELECTRONIC\s+SYSTEMS?\s*\w*|EQUIPMENT\s+SYSTEMS?\s*\w*|'
    r'AEROSPACE[-\s]+\w+|'  # "AEROSPACE-TUCSON", "AEROSPACE DIVISION"
    r'SERVICE\s+AND\s+SUPPORT\s+\w+|'
    r'COMMAND\s+AMSTA[-\w]*|'
    r'\w+\s+SITE|'  # "EATONTOWN SITE"
    r'DIV\s+\w+.*'  # anything after "DIV"
    r')$',
    re.IGNORECASE,
)

# Mc/Mac prefix patterns
_MC_PATTERN = re.compile(r'\bMc([a-z])', re.ASCII)

# Words to keep lowercase in title case (except at start)
_LOWERCASE_WORDS = {'OF', 'THE', 'AND', 'FOR', 'IN', 'ON', 'AT', 'TO', 'A', 'AN'}

# Generic/department words — if ALL significant words in a DBA name are generic,
# it's a department description, not a brand name
_GENERIC_WORDS = {
    'SERVICE', 'SERVICES', 'SUPPORT', 'OPERATION', 'OPERATIONS',
    'DIVISION', 'DEPARTMENT', 'FACILITY', 'CENTER', 'BRANCH',
    'OFFICE', 'UNIT', 'SECTION', 'PROGRAM', 'PROJECT',
    'COMMAND', 'DIRECTORATE', 'BUREAU', 'AGENCY',
    'AND', 'THE', 'OF', 'FOR', 'IN', 'ON', 'AT', 'TO', 'A', 'AN',
}


def _smart_title(text):
    """Title-case with small-word awareness and Mc/Mac handling."""
    words = text.split()
    result = []
    for i, word in enumerate(words):
        upper = word.upper().rstrip('.,;:')
        if i > 0 and upper in _LOWERCASE_WORDS:
            result.append(word.lower())
        elif '-' in word:
            parts = word.split('-')
            result.append('-'.join(p.capitalize() for p in parts))
        else:
            result.append(word.capitalize())
    text = ' '.join(result)
    # Fix Mc* capitalization: Mcdonnell -> McDonnell
    text = _MC_PATTERN.sub(lambda m: f'Mc{m.group(1).upper()}', text)
    return text


def _strip_legal(name):
    """Repeatedly strip trailing legal suffixes."""
    for _ in range(3):
        cleaned = _LEGAL_PATTERN.sub('', name).rstrip('., ')
        if cleaned == name:
            break
        name = cleaned
    return name


def format_manufacturer_name(raw_name):
    """
    Beautify a DLA CAGE manufacturer name for display.

    Strategy:
    1. If DBA present, take the DBA portion (operating/trade name)
    2. If DIV present, take portion before DIV
    3. Strip legal suffixes (Inc, Corp, LLC, etc.)
    4. Strip department/location noise for cleaner names
    5. Apply smart title case
    """
    if not raw_name:
        return ""

    name = raw_name.strip()

    # 1. Extract base name (before DBA or DIV)
    base_name = re.split(r'\b(?:DBA|DIV)\b', name, flags=re.IGNORECASE)[0].strip()
    base_name = _strip_legal(base_name).rstrip('., ')

    # 2. If DBA is present, try the DBA portion (operating/trade name)
    dba_match = re.search(r'\bDBA\b\s+(.+)', name, re.IGNORECASE)
    if dba_match:
        dba_name = dba_match.group(1).strip()
        # DBA portion may have DIV — take before DIV
        div_match = re.search(r'\bDIV\b', dba_name, re.IGNORECASE)
        if div_match:
            dba_name = dba_name[:div_match.start()].strip()
        dba_name = _strip_legal(dba_name)
        dba_name = _DEPT_SUFFIXES.sub('', dba_name).strip().rstrip('., ')
        # Use DBA name if it looks like a real company name, not a department
        # A real company name: shares words with the base, or is a known brand
        dba_upper = set(dba_name.upper().split())
        base_upper = set(base_name.upper().split()) - {'INC', 'CORP', 'LLC', 'CO', 'THE'}
        has_overlap = bool(dba_upper & base_upper)
        if has_overlap and len(dba_name.split()) >= 2:
            return _smart_title(dba_name)
        # Single well-known brand word from DBA (e.g. "PNEUTRONICS")
        # But skip if it's all generic/department words
        dba_words_upper = {w.upper() for w in dba_name.split()}
        is_generic = dba_words_upper and dba_words_upper <= _GENERIC_WORDS
        if len(dba_name) > 5 and not has_overlap and not is_generic:
            return _smart_title(dba_name)
        # Fall through to base name

    # 3. Clean up base name — strip mid-name legal words and everything after
    mid_match = _MID_LEGAL.search(base_name)
    if mid_match:
        base_name = base_name[:mid_match.start()].strip()

    # Strip trailing legal suffixes
    base_name = _strip_legal(base_name).rstrip('., ')

    # Strip department suffixes
    base_name = _DEPT_SUFFIXES.sub('', base_name).strip().rstrip('., ')

    # 4. Strip leading "THE"
    base_name = re.sub(r'^THE\s+', '', base_name, flags=re.IGNORECASE).strip()

    # 5. Smart title case
    return _smart_title(base_name)


def format_nomenclature(raw_name):
    """
    Beautify a DLA item nomenclature for product display.

    Strategy:
    1. Fix comma spacing: "CAP,FILLER" -> "Cap, Filler"
    2. Apply smart title case
    """
    if not raw_name:
        return ""

    name = raw_name.strip()

    # Fix comma spacing: "WORD,WORD" -> "WORD, WORD"
    name = re.sub(r',(?=\S)', ', ', name)

    # Smart title case
    return _smart_title(name)

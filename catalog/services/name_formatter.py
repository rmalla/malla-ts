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

# ---------------------------------------------------------------------------
# Legal suffix registry: (label, regex_fragment) tuples
# Compound suffixes MUST come before single-word to ensure greedy matching.
# ---------------------------------------------------------------------------
_COMPOUND_SUFFIXES = [
    ('GMBH & CO. KGAA', r'GMBH\s*&\s*CO\.?\s*KGAA'),
    ('GMBH & CO. KG',  r'GMBH\s*&\s*CO\.?\s*KG'),
    ('S DE RL DE CV',   r'S\s+DE\s+RL\s+DE\s+CV'),
    ('SAPI DE CV',      r'SAPI\s+DE\s+CV'),
    ('SA DE CV',        r'SA\s+DE\s+CV'),
    ('PTY LTD',         r'PTY\.?\s+LTD\.?'),
    ('PVT LTD',         r'PVT\.?\s+LTD\.?'),
    ('SDN BHD',         r'SDN\.?\s+BHD\.?'),
    ('CO LTD',          r'CO\.?\s+LTD\.?'),
]

_SINGLE_SUFFIXES = [
    # English
    'INC', 'INCORPORATED', 'CORP', 'CORPORATION', 'LLC', 'LTD', 'LIMITED',
    'CO', 'COMPANY', 'ENTERPRISES', 'ENTERPRISE', 'INTL',
    'PLC', 'LLP', 'LLLP', 'LP',
    # German
    'GMBH', 'AG', 'KG', 'KGAA', 'OHG', 'GBR',
    # French
    'SAS', 'SARL', 'EURL', 'SNC',
    # Italian / Romanian
    'SRL', 'SPA',
    # Dutch
    'BV', 'NV', 'VOF', 'CV',
    # Nordic
    'AB', 'HB', 'KB', 'OY', 'OYJ', 'AS', 'ASA', 'APS',
    # Latin American
    'LTDA', 'SA',
    # Australian
    'PTY',
    # Other
    'PTE', 'KK',
]

# Build one combined regex: compound first, then single-word
_suffix_fragments = (
    [frag for _, frag in _COMPOUND_SUFFIXES] +
    [rf'{re.escape(s)}\.?' for s in _SINGLE_SUFFIXES] +
    # Dotted forms like L.L.C., L.P.
    [r'L\.?L\.?C\.?', r'L\.?P\.?']
)

_LEGAL_PATTERN = re.compile(
    r'[,.]?\s*\b(' + '|'.join(_suffix_fragments) + r')\b\.?\s*$',
    re.IGNORECASE,
)

# Legal words that appear mid-name — strip along with everything after.
# Negative lookahead: don't strip when followed by DBA/DIV or & (for GMBH & CO. KG).
_MID_LEGAL = re.compile(
    r'[,.]?\s*\b(INC\.?|CORP\.?|CORPORATION|GMBH|AG)\b\.?[,.]?\s+(?!DBA|DIV|&)',
    re.IGNORECASE,
)

# Multi-word department/site suffixes to strip
_DEPT_SUFFIXES = re.compile(
    r'\s+('
    r'GOVERNMENT\s+SYSTEMS|DEFENSE\s+\w+|'
    r'ELECTRONIC\s+SYSTEMS?\s*\w*|EQUIPMENT\s+SYSTEMS?\s*\w*|'
    r'AEROSPACE[-\s]+\w+|'
    r'SERVICE\s+AND\s+SUPPORT\s+\w+|'
    r'COMMAND\s+AMSTA[-\w]*|'
    r'DEPT\s+OF\s+.*|'
    r'DIVISION\s+OF\s+.*|'
    r'SUBSIDIARY\s+OF\s+.*|SUB\s+OF\s+.*|'
    r'\w+\s+SITE|'
    r'DIV\s+\w+.*'
    r')$',
    re.IGNORECASE,
)

# Dotted acronym pattern: A.B.C or A.B.C. (single letters separated by dots)
_DOTTED_ACRONYM = re.compile(r'^[A-Za-z](?:\.[A-Za-z])+\.?$')

# Mc/Mac prefix patterns
_MC_PATTERN = re.compile(r'\bMc([a-z])', re.ASCII)

# Words to keep lowercase in title case (except at start)
_LOWERCASE_WORDS = {
    'OF', 'THE', 'AND', 'FOR', 'IN', 'ON', 'AT', 'TO', 'AN',
    # French articles/prepositions
    'DE', 'DES', 'DU', 'LA', 'LE', 'LES', 'AU', 'AUX',
}

# Generic/department words — if ALL significant words in a DBA name are generic,
# it's a department description, not a brand name
_GENERIC_WORDS = {
    'SERVICE', 'SERVICES', 'SUPPORT', 'OPERATION', 'OPERATIONS',
    'DIVISION', 'DEPARTMENT', 'FACILITY', 'CENTER', 'BRANCH',
    'OFFICE', 'UNIT', 'SECTION', 'PROGRAM', 'PROJECT',
    'COMMAND', 'DIRECTORATE', 'BUREAU', 'AGENCY',
    'AND', 'THE', 'OF', 'FOR', 'IN', 'ON', 'AT', 'TO', 'A', 'AN',
}

# Acronym detection: 2-3 char all-uppercase words, or mixed alpha-digit tokens (3M, BAE, ABB)
_ACRONYM_RE = re.compile(r'^[A-Z0-9]{2,3}$')
# Known acronyms that are 4+ chars
_KNOWN_ACRONYMS = {'BASF', 'EADS'}
# Common short words that are NOT acronyms — should be title-cased normally
_NOT_ACRONYMS = {
    # English words
    'AIR', 'ALL', 'AMP', 'AND', 'ANY', 'ARC', 'ARM', 'ART', 'AXE',
    'BAD', 'BAG', 'BAR', 'BAY', 'BED', 'BIG', 'BIN', 'BIT', 'BOW', 'BOX', 'BUS', 'BUT', 'BUY',
    'CAB', 'CAM', 'CAN', 'CAP', 'CAR', 'CAT', 'COG', 'CUP', 'CUT',
    'DAM', 'DAY', 'DES', 'DID', 'DIE', 'DIG', 'DOG', 'DOT', 'DRY', 'DUE',
    'EAR', 'EAT', 'END', 'EYE',
    'FAN', 'FAR', 'FAT', 'FED', 'FEW', 'FIG', 'FIN', 'FIT', 'FIX', 'FLY', 'FOR', 'FOX', 'FUN', 'FUR',
    'GAP', 'GAS', 'GET', 'GOT', 'GUM', 'GUN', 'GUT',
    'HAD', 'HAS', 'HAT', 'HER', 'HIM', 'HIS', 'HIT', 'HOT', 'HOW', 'HUB',
    'ICE', 'ILL',
    'ION', 'JAM', 'JAR', 'JAW', 'JET', 'JOB', 'JOY',
    'KEY', 'KIT',
    'LAB', 'LAP', 'LAW', 'LAY', 'LED', 'LEG', 'LES', 'LET', 'LID', 'LIT', 'LOG', 'LOT', 'LOW', 'LUG',
    'MAN', 'MAP', 'MAT', 'MAY', 'MEN', 'MET', 'MID', 'MIX', 'MOD',
    'NET', 'NEW', 'NIT', 'NOR', 'NOT', 'NOW', 'NUT',
    'OAK', 'ODD', 'OFF', 'OIL', 'OLD', 'ONE', 'OUR', 'OUT', 'OWE', 'OWN',
    'PAD', 'PAN', 'PAY', 'PEN', 'PER', 'PET', 'PIN', 'PIT', 'PLY', 'POD', 'POT', 'PRE', 'PRO', 'PUT',
    'RAM', 'RAN', 'RAW', 'RED', 'RIB', 'RIG', 'RIM', 'ROD', 'ROW', 'RUB', 'RUG', 'RUN',
    'SAT', 'SAW', 'SAY', 'SEA', 'SET', 'SHE', 'SIT', 'SIX', 'SKI', 'SKY', 'SOD',
    'SON', 'SPA', 'SPY', 'SUM', 'SUN',
    'TAB', 'TAG', 'TAN', 'TAP', 'TAR', 'TAX', 'TEE', 'TEN', 'THE', 'TIE', 'TIN', 'TIP', 'TOE', 'TON', 'TOO', 'TOP', 'TOW', 'TOY', 'TUB', 'TWO',
    'URN', 'USE',
    'VAN', 'VAT', 'VET',
    'WAR', 'WAX', 'WAY', 'WEB', 'WET', 'WHO', 'WHY', 'WIN', 'WIT', 'WON', 'WOO',
    'YAM', 'YET', 'YOU',
    'ZEN', 'ZIP', 'ZOO',
    # Common industry words
    'MFG', 'MFR', 'DIV', 'SUB', 'INC', 'AVE',
    # Two-letter words
    'AD', 'AM', 'AN', 'AS', 'AT', 'BE', 'BY', 'DO', 'GO', 'HE',
    'IF', 'IN', 'IS', 'IT', 'ME', 'MY', 'NO', 'OF', 'OH', 'OK',
    'ON', 'OR', 'OX', 'SO', 'TO', 'UP', 'US', 'WE',
}


def _is_acronym(word):
    """Return True if word looks like an acronym (not a common English word)."""
    clean = word.upper().rstrip('.,;:')
    if clean in _KNOWN_ACRONYMS:
        return True
    if clean in _NOT_ACRONYMS:
        return False
    return bool(_ACRONYM_RE.match(clean))


def _smart_title(text):
    """Title-case with small-word awareness, Mc/Mac handling, and acronym preservation."""
    words = text.split()
    result = []
    for i, word in enumerate(words):
        upper = word.upper().rstrip('.,;:')
        # Dotted acronyms: E.C.A, U.S.A. -> uppercase
        if _DOTTED_ACRONYM.match(word):
            result.append(word.upper())
        # Parenthesized words: (UK), (AUSTRALIA)
        elif word.startswith('(') and word.endswith(')'):
            inner = word[1:-1]
            if _is_acronym(inner.upper()) and inner == inner.upper():
                result.append(f'({inner.upper()})')
            else:
                result.append(f'({inner.capitalize()})')
        elif _is_acronym(word.upper()) and word == word.upper():
            # Preserve acronyms as uppercase: ABB, 3M, IBM
            result.append(word.upper())
        elif i > 0 and upper in _LOWERCASE_WORDS:
            result.append(word.lower())
        elif '-' in word:
            parts = word.split('-')
            titled = []
            for p in parts:
                if _is_acronym(p.upper()) and p == p.upper():
                    titled.append(p.upper())
                else:
                    titled.append(p.capitalize())
            result.append('-'.join(titled))
        else:
            result.append(word.capitalize())
    text = ' '.join(result)
    # Fix Mc* capitalization: Mcdonnell -> McDonnell
    text = _MC_PATTERN.sub(lambda m: f'Mc{m.group(1).upper()}', text)
    return text


def _strip_legal(name):
    """Repeatedly strip trailing legal suffixes."""
    for _ in range(5):
        cleaned = _LEGAL_PATTERN.sub('', name).rstrip('., ')
        if cleaned == name or len(cleaned) < 2:
            break
        name = cleaned
    return name


def _handle_dash_separator(name):
    """
    Handle ` - ` separated patterns in manufacturer names.

    - Location suffix: "APPLIED COMPOSITES - INDIANAPOLIS" -> "APPLIED COMPOSITES"
    - Translation pattern: prefer the shorter/English side
    """
    if ' - ' not in name:
        return name

    parts = name.split(' - ', 1)
    left = parts[0].strip()
    right = parts[1].strip()

    if not left or not right:
        return name

    right_words = right.split()
    left_words = left.split()

    # Both sides are multi-word ALL-CAPS (translation pattern) -> prefer right side (English)
    if (len(left_words) >= 2 and len(right_words) >= 2 and
            left == left.upper() and right == right.upper()):
        return right

    # Short location suffix (1-2 uppercase words on the right) -> take left side
    if len(right_words) <= 2 and all(w.isupper() or w.replace('.', '').isupper() for w in right_words):
        return left

    return name


def _clean_display_name(text):
    """
    Post-processing cleanup for manufacturer display names.

    1. Collapse dotted initials: A.B.M. -> ABM, A.B.Z. -> ABZ
    2. Collapse ampersand between single letters: A&B -> AB, A & B -> AB
    3. Replace word-level ampersand with 'and': Machining & Fab -> Machining and Fab
    4. Collapse spaced single letters: H B -> HB, A C I -> ACI
    5. Clean up leftover punctuation/spacing
    """
    if not text:
        return text

    # 1. Dotted initials: "A.B.M.", "A. B. M.", "A.K.O" -> "ABM", "AKO"
    #    2+ single-letter-dot pairs, optional trailing letter without dot
    def collapse_dotted(m):
        letters = re.findall(r'[A-Za-z]', m.group(0))
        return ''.join(letters).upper()
    text = re.sub(r'(?<!\w)([A-Za-z]\.\s*){2,}([A-Za-z])?(?!\w)', collapse_dotted, text)

    # 2. Ampersand between single letters: "A&B", "A & B" -> "AB"
    text = re.sub(r'(?<!\w)([A-Za-z])\s*&\s*([A-Za-z])(?!\w)', lambda m: m.group(1).upper() + m.group(2).upper(), text)

    # 3. Word-level ampersand -> "and"
    text = re.sub(r'\s*&\s*', ' and ', text)

    # 4. Strip "a/an UNIT/DIVISION/DIV/DEPT/SUB OF ..." noise phrases
    text = re.sub(r'\s+an?\s+(?:unit|division|div|dept|sub)\s+of\s+.*$', '', text, flags=re.IGNORECASE)
    # Re-strip trailing legal suffixes exposed by noise removal (CO, LP, etc.)
    text = _strip_legal(text.upper()).rstrip('., ')
    # Re-apply smart title case after stripping
    text = _smart_title(text)

    # 5. Collapse consecutive spaced single letters: "H B" -> "HB", "T T R" -> "TTR"
    #    "A" merges with other single letters but NOT with multi-char tokens
    #    (to avoid "CO A" -> "COA" or "Pump A TBG" -> "Pump ATBG")
    prev = None
    while prev != text:
        prev = text
        # Merge two standalone single letters (including A)
        text = re.sub(r'(?<!\w)([A-Z]) ([A-Z])(?!\w)', r'\1\2', text)
        # Merge trailing single letter onto short all-caps token: "RD L" -> "RDL"
        # Skip A to avoid "CO A" -> "COA" (article not initial)
        text = re.sub(r'\b([A-Z]{2,4}) ([B-Z])(?!\w)', r'\1\2', text)
        # Merge leading single letter into following short all-caps token: "T TR" -> "TTR"
        # Skip A to avoid "A TBG" -> "ATBG"
        text = re.sub(r'(?<!\w)([B-Z]) ([A-Z]{2,4})\b', r'\1\2', text)
    # Final pass: merge trailing "A" onto all-caps token when followed by a regular word or end
    # "US A" -> "USA", "AA A Engineering" -> "AAA Engineering"
    # But not "Pump A TBG" (A between mixed-case and all-caps)
    text = re.sub(r'\b([A-Z]{2,4}) A(?=\s+[A-Z][a-z]|\s*$)', r'\1A', text)

    # 5. Strip trailing filler words: "Antunes and", "Products of"
    text = re.sub(r'\s+(?:and|of|the|for|or|a)\s*$', '', text, flags=re.IGNORECASE)

    # 6. Strip trailing punctuation and extra spaces
    text = re.sub(r'[,.\s/]+$', '', text)
    text = re.sub(r'\s{2,}', ' ', text).strip()

    return text


def format_manufacturer_name(raw_name):
    """
    Beautify a DLA CAGE manufacturer name for display.

    Strategy:
    1. Handle dash-separator patterns (locations, translations)
    2. If DBA present, take the DBA portion (operating/trade name)
    3. If DIV present, take portion before DIV
    4. Strip legal suffixes (Inc, Corp, LLC, GMBH, AG, etc.)
    5. Strip mid-name legal words
    6. Strip department/location noise
    7. Strip leading "THE"
    8. Apply smart title case with acronym preservation
    """
    if not raw_name:
        return ""

    name = raw_name.strip()

    # 1. Extract base name (before DBA, DIV, or DIVISION) — run before dash handler
    base_name = re.split(r'\b(?:DBA|DIVISION|DIV)\b', name, flags=re.IGNORECASE)[0].strip()
    # Strip trailing articles left over from "CO A DIV" splits
    base_name = re.sub(r'\s+[Aa]\s*$', '', base_name)

    # 2. Handle dash-separator patterns
    base_name = _handle_dash_separator(base_name)
    base_name = _strip_legal(base_name).rstrip('., ')

    # 3. If DBA is present, try the last DBA portion (operating/trade name)
    dba_match = re.search(r'^.*\bDBA\b\s+(.+)', name, re.IGNORECASE)
    if dba_match:
        dba_name = dba_match.group(1).strip()
        # DBA portion may have DIV — take before DIV
        div_match = re.search(r'\bDIV\b', dba_name, re.IGNORECASE)
        if div_match:
            dba_name = dba_name[:div_match.start()].strip()
        dba_name = _strip_legal(dba_name)
        dba_name = _DEPT_SUFFIXES.sub('', dba_name).strip().rstrip('., ')
        # Use DBA name if it looks like a real company name, not a department
        dba_upper = set(dba_name.upper().split())
        base_upper = set(base_name.upper().split()) - {'INC', 'CORP', 'LLC', 'CO', 'THE'}
        has_overlap = bool(dba_upper & base_upper)
        if has_overlap and len(dba_name.split()) >= 2:
            return _clean_display_name(_smart_title(dba_name))
        dba_words_upper = {w.upper() for w in dba_name.split()}
        is_generic = dba_words_upper and dba_words_upper <= _GENERIC_WORDS
        if len(dba_name) > 5 and not has_overlap and not is_generic:
            return _clean_display_name(_smart_title(dba_name))

    # 4. Clean up base name — strip mid-name legal words and everything after
    mid_match = _MID_LEGAL.search(base_name)
    if mid_match:
        base_name = base_name[:mid_match.start()].strip()

    # Strip trailing legal suffixes
    base_name = _strip_legal(base_name).rstrip('., ')

    # 5. Strip department suffixes
    base_name = _DEPT_SUFFIXES.sub('', base_name).strip().rstrip('., ')

    # 6. Strip leading/trailing "THE"
    base_name = re.sub(r'^THE\s+', '', base_name, flags=re.IGNORECASE).strip()
    base_name = re.sub(r'[,\s]+THE$', '', base_name, flags=re.IGNORECASE).strip()

    # 7. Second pass of legal suffix stripping (catches suffixes exposed by THE removal)
    base_name = _strip_legal(base_name).rstrip('., ')

    # 8. Smart title case + display cleanup
    return _clean_display_name(_smart_title(base_name))


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


def naturalize_nomenclature(raw_name):
    """
    Convert military nomenclature to natural English product name.

    MIL-STD nomenclature: BASE_NOUN, MODIFIER1, MODIFIER2
    Natural English:       MODIFIER1 MODIFIER2 BASE_NOUN

    Examples:
        "HOSE ASSEMBLY,NONMETALLIC"        -> "Nonmetallic Hose Assembly"
        "MOTOR,DIRECT CURRENT"             -> "Direct Current Motor"
        "SEAT,VALVE"                       -> "Valve Seat"
        "CIRCUIT CARD ASSEMBLY"            -> "Circuit Card Assembly"
        "COVER,HYDRAULIC,PUMP-MOTOR"       -> "Hydraulic Pump-Motor Cover"
        "RECTIFIER,SEMICONDUCTOR DEVICE,U" -> "Semiconductor Device Rectifier"
    """
    if not raw_name:
        return ""

    name = raw_name.strip()

    # Fix comma spacing for consistent splitting
    name = re.sub(r',(?=\S)', ', ', name)

    if ',' not in name:
        return _smart_title(name)

    parts = [p.strip() for p in name.split(',')]
    parts = [p for p in parts if p]

    if len(parts) < 2:
        return _smart_title(name)

    base = parts[0]
    modifiers = parts[1:]

    # Drop truncated fragments: single char or empty after strip
    modifiers = [m for m in modifiers if len(m) > 1]

    if not modifiers:
        return _smart_title(base)

    # Natural order: modifiers before base noun
    natural = ' '.join(modifiers) + ' ' + base
    return _smart_title(natural)

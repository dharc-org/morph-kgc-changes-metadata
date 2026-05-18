# -*- coding: utf-8 -*-
"""
Shared utility functions for CHAD-ASK.

Functions here were previously duplicated across demo scripts.
They are collected here so every generator imports from one place.
"""

import re
import unicodedata
from constants import PREFIXES


# ---------------------------------------------------------------------------
# Text normalisation
# ---------------------------------------------------------------------------

def normalize_text(value: str) -> str:
    """
    Normalise a string for fuzzy key matching.

    Applies NFKC unicode normalisation, replaces escaped slashes and
    non-breaking spaces, lowercases, strips, and collapses whitespace.
    """
    value = unicodedata.normalize("NFKC", str(value))
    value = value.replace("\\/", "/")
    value = value.replace("\u00A0", " ")
    value = value.lower().strip()
    value = re.sub(r"\s+", " ", value)
    return value


# ---------------------------------------------------------------------------
# JSON key resolution
# ---------------------------------------------------------------------------

def find_key_by_fragments(row: dict, required_fragments: list[str]) -> str | None:
    """
    Return the first key in *row* whose normalised form contains all
    *required_fragments* (each also normalised). Returns None if no match.
    """
    normalised_fragments = [normalize_text(f) for f in required_fragments]
    for key in row:
        nk = normalize_text(key)
        if all(fragment in nk for fragment in normalised_fragments):
            return key
    return None


def get_value(
    row: dict,
    exact_key: str,
    fragments: list[str],
    fallback: str,
    *,
    use_fallback: bool = True,
    label: str = "",
) -> str:
    """
    Resolve a questionnaire value from *row* using a three-level strategy:

    1. Exact key match (``exact_key``).
    2. Fuzzy fragment match (``fragments``).
    3. Hardcoded fallback (``fallback``) when ``use_fallback`` is True.

    Raises ``KeyError`` if none of the above succeeds and *use_fallback*
    is False.
    """
    if exact_key in row:
        return str(row[exact_key]).strip()

    fuzzy = find_key_by_fragments(row, fragments)
    if fuzzy is not None:
        return str(row[fuzzy]).strip()

    if use_fallback:
        return fallback

    raise KeyError(
        f"Could not resolve value for '{label or exact_key}'. "
        f"Update the corresponding JSON_KEYS entry with the real "
        f"LimeSurvey export key."
    )


# ---------------------------------------------------------------------------
# Boolean helpers
# ---------------------------------------------------------------------------

def value_is_yes(value) -> bool:
    """Return True if *value* represents an affirmative answer."""
    return str(value).strip().lower() in {"yes", "true", "1", "y"}


def get_binary_answer(
    row: dict,
    yes_key: str,
    no_key: str,
    yes_fragments: list[str],
    no_fragments: list[str],
    fallback: bool,
    *,
    use_fallback: bool = True,
    label: str = "",
) -> bool:
    """
    Read a Yes/No answer from two LimeSurvey export keys.

    LimeSurvey encodes a single-choice Yes/No question as two separate
    boolean columns (one for each option). This function handles both the
    exact-key and fuzzy-fragment resolution strategies, with an optional
    fallback for when the real export is not yet available.

    Returns True if the *yes* key is selected, False otherwise.
    """
    if yes_key in row and value_is_yes(row[yes_key]):
        return True
    if no_key in row and value_is_yes(row[no_key]):
        return False

    fuzzy_yes = find_key_by_fragments(row, yes_fragments)
    if fuzzy_yes is not None and value_is_yes(row[fuzzy_yes]):
        return True

    fuzzy_no = find_key_by_fragments(row, no_fragments)
    if fuzzy_no is not None and value_is_yes(row[fuzzy_no]):
        return False

    if use_fallback:
        return fallback

    raise KeyError(
        f"Could not determine the binary answer for '{label}'. "
        f"Update the corresponding JSON_KEYS entries."
    )


# ---------------------------------------------------------------------------
# YARRRML helpers
# ---------------------------------------------------------------------------

def build_prefixes_block() -> str:
    """Return the YARRRML prefixes block as a multi-line string."""
    lines = ["prefixes:"]
    for prefix, uri in PREFIXES:
        lines.append(f'  {prefix}: "{uri}"')
    return "\n".join(lines)
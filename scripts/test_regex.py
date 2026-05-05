import re

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

month_names = "|".join(MONTH_MAP.keys())
print(f"Python version: {__import__('sys').version}")
print(f"Month names length: {len(month_names)}")

# Test the exact pattern used in _extract_date
try:
    pattern = rf"({month_names})\s+(\d{{1,2}})(?:\s*,?\s*(\d{{4}}))?"
    print(f"Pattern compiled OK, length: {len(pattern)}")
    c = re.compile(pattern)
    test = "march 5 2026"
    matches = list(c.finditer(test))
    print(f"Test 'march 5 2026': {len(matches)} matches")
except re.error as e:
    print(f"Regex error: {e}")
    print(f"Pattern: {pattern}")

# Test the other regex patterns
try:
    re.compile(r"(\d+)\s*°?\s*f")
    print("temp regex OK")
except re.error as e:
    print(f"temp regex error: {e}")

try:
    re.compile(r"(\d+)\s*degrees")
    print("degrees regex OK")
except re.error as e:
    print(f"degrees regex error: {e}")

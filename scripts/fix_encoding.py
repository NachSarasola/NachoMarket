"""Fix double-encoded UTF-8 in bot.py and weather.py."""
import re

replacements = [
    ("\u00e2\u20ac\u201c", "\u2014"),   # â€" → EM DASH
    ("\u00e2\u20ac\u201d", "\u2014"),   # â€" variant
    ("\u00e2\u20ac\u2122", "\u2122"),   # â„¢ → ™
    ("\u00c3\u00b1", "\u00f1"),         # Ã± → ñ
    ("\u00c3\u00b3", "\u00f3"),         # Ã³ → ó
    ("\u00c3\u00ad", "\u00ed"),         # Ã­ → í
    ("\u00c3\u00ba", "\u00fa"),         # Ãº → ú
    ("\u00c3\u00a1", "\u00e1"),         # Ã¡ → á
    ("\u00c3\u00a9", "\u00e9"),         # Ã© → é
    ("\u00c3\u0161", "\u00da"),         # Ãš → Ú
    ("\u00c2\u00b0", "\u00b0"),         # Â° → °
]

for path in ["src/telegram/bot.py", "src/strategy/weather.py"]:
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    for old, new in replacements:
        content = content.replace(old, new)

    # Aggressive: any â€X → EM DASH, any Ã²X → correct latin-1 char
    content = re.sub("\u00e2\u20ac.", "\u2014", content)
    content = re.sub("\u00c3\u00b2", "\u00f2", content)  # Ã² → ò
    content = re.sub("\u00c3\u0081", "\u00c1", content)  # partial fix
    content = re.sub("\u00c2\u00a0", " ", content)       # nbsp mojibake
    content = re.sub("\u00c2-", "-", content)             # bullet mojibake

    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

    # Verify
    import ast
    try:
        ast.parse(content)
        print(f"{path}: Python syntax OK")
    except SyntaxError as e:
        print(f"{path}: SYNTAX ERROR at line {e.lineno}: {e.msg}")

    # Check remaining mojibake
    bad = ["\u00e2\u20ac", "\u00c3"]  # simplified check
    found = sum(1 for b in bad if b in content)
    print(f"{path}: {found} mojibake sequences remain")

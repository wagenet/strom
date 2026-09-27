"""Minimal Rust item splitter: enough lexing to find item boundaries.

Splits a block of Rust source into items. Each item carries the comments,
attributes and blank lines that precede it, so concatenating the items of a
block reproduces the block byte for byte.
"""

import re

ITEM_RE = re.compile(
    r"\s*(?:pub(?:\([^)]*\))?\s+)?(?:(?:const|async|unsafe|extern\s+\"[^\"]*\")\s+)*"
    r"(fn|struct|enum|const|static|type|trait|impl|mod|use|macro_rules!|union)\b"
)


def _skip_ws_comments(src, i):
    n = len(src)
    while i < n:
        if src[i].isspace():
            i += 1
        elif src.startswith("//", i):
            j = src.find("\n", i)
            i = n if j < 0 else j + 1
        elif src.startswith("/*", i):
            i = _skip_block_comment(src, i)
        elif src.startswith("#[", i) or src.startswith("#![", i):
            i = _skip_group(src, src.index("[", i))
        else:
            break
    return i


def _skip_block_comment(src, i):
    depth = 0
    n = len(src)
    while i < n:
        if src.startswith("/*", i):
            depth += 1
            i += 2
        elif src.startswith("*/", i):
            depth -= 1
            i += 2
            if depth == 0:
                return i
        else:
            i += 1
    raise ValueError("unterminated block comment")


def _skip_string(src, i):
    # src[i] is '"'
    i += 1
    while src[i] != '"':
        i += 2 if src[i] == "\\" else 1
    return i + 1


def _skip_raw_string(src, i):
    m = re.compile(r'b?r(#*)"').match(src, i)
    end = '"' + m.group(1)
    j = src.index(end, m.end())
    return j + len(end)


def _skip_char_or_lifetime(src, i):
    # src[i] is '\''
    m = re.compile(r"'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F]+\}|.)|[^\\'])'").match(src, i)
    if m:
        return m.end()
    return i + 1  # lifetime


def tokens_scan(src, i, stop):
    """Advance from i over code, calling stop(ch, depth, pos) at each
    structural char; returns position where stop returned True."""
    depth = 0
    n = len(src)
    while i < n:
        c = src[i]
        if src.startswith("//", i):
            j = src.find("\n", i)
            i = n if j < 0 else j + 1
            continue
        if src.startswith("/*", i):
            i = _skip_block_comment(src, i)
            continue
        if re.compile(r'b?r#*"').match(src, i) and (i == 0 or not (src[i - 1].isalnum() or src[i - 1] == "_")):
            i = _skip_raw_string(src, i)
            continue
        if c == '"':
            i = _skip_string(src, i)
            continue
        if c == "'":
            i = _skip_char_or_lifetime(src, i)
            continue
        if c in "{[(":
            depth += 1
        elif c in "}])":
            depth -= 1
        if stop(c, depth, i):
            return i
        i += 1
    raise ValueError("ran off the end")


def _skip_group(src, i):
    return tokens_scan(src, i, lambda c, d, p: d == 0 and c in "}])") + 1


def split_items(src):
    """Return a list of (kind, name, text) covering src exactly.
    A trailing chunk of only whitespace/comments has kind None."""
    items = []
    pos = 0
    n = len(src)
    while pos < n:
        start = _skip_ws_comments(src, pos)
        if start >= n:
            items.append((None, None, src[pos:]))
            break
        m = ITEM_RE.match(src, start)
        if not m:
            raise ValueError("unrecognised item at: %r" % src[start:start + 80])
        kind = m.group(1)
        if kind in ("const", "static", "type", "use"):
            end = tokens_scan(src, start, lambda c, d, p: d == 0 and c == ";") + 1
        else:
            end = tokens_scan(src, start, lambda c, d, p: d == 0 and c in ";}") + 1
        # consume rest of the line (trailing comment / newline)
        nl = src.find("\n", end)
        end = n if nl < 0 else nl + 1
        text = src[pos:end]
        items.append((kind, item_name(kind, src[start:end]), text))
        pos = end
    return items


def item_name(kind, text):
    head = text.split("{", 1)[0]
    if kind == "impl":
        h = re.sub(r"\s+", " ", ITEM_RE.sub("", head, count=1).strip())
        h = re.sub(r"^<[^>]*>\s*", "", h)
        return h.split(" where ")[0].strip()
    if kind == "use":
        return re.sub(r"\s+", " ", text.strip())
    if kind == "macro_rules!":
        return re.search(r"macro_rules!\s*(\w+)", head).group(1)
    m = re.search(r"\b" + re.escape(kind) + r"\s+(\w+)", head)
    return m.group(1)


def body_span(text):
    """For an item with a brace body, return (open, close) indices of the
    outer braces."""
    o = tokens_scan(text, 0, lambda c, d, p: c == "{" and d == 1)
    c = tokens_scan(text, o, lambda ch, d, p: d == 0 and ch == "}")
    return o, c

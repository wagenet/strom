import rsitems as R

def has_body(kind, text):
    if kind not in ("impl", "mod"):
        return False
    try:
        R.body_span(text)
        return True
    except Exception:
        return False

def keyed(src, top_container=""):
    """Yield (container, key, text, kind) for leaf items; containers are
    impl blocks and `mod tests`."""
    out = []
    for kind, name, text in R.split_items(src):
        if kind is None or kind == "use":
            out.append((top_container, None if kind is None else "use", text, kind))
            continue
        if has_body(kind, text) and (kind == "impl" or name == "tests"):
            o, c = R.body_span(text)
            cont = ("impl " + name) if kind == "impl" else "tests"
            for kk, nn, tt in R.split_items(text[o + 1:c]):
                out.append((cont, None if kk is None else ("use" if kk == "use" else kk + " " + nn), tt, kk))
            continue
        out.append((top_container, kind + " " + name, text, kind))
    return out

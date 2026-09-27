"""Flatten and regroup Rust `use` declarations."""

import re


def flatten(use):
    """`use a::{b, c::{d as e}};` -> ["a::b", "a::c::d as e"]"""
    body = re.sub(r"\s+", " ", use.strip())
    body = re.sub(r"^use ", "", body).rstrip(";").strip()
    out = []

    def walk(prefix, s):
        s = s.strip()
        if not s:
            return
        i = s.find("{")
        if i < 0:
            out.append(prefix + s)
            return
        assert s.endswith("}"), s
        head = s[:i]
        inner = s[i + 1:-1]
        depth, start = 0, 0
        for j, c in enumerate(inner + ","):
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
            elif c == "," and depth == 0:
                walk(prefix + head, inner[start:j])
                start = j + 1

    walk("", body)
    return [p.replace("::self", "") for p in out]


def group(paths):
    """Inverse of flatten, one level deep: one `use` per parent path."""
    by = {}
    for p in paths:
        base = p.split(" as ")[0]
        if "::" in base:
            parent, leaf = p.rsplit("::", 1) if " as " not in p else (base.rsplit("::", 1)[0], p[len(base.rsplit("::", 1)[0]) + 2:])
        else:
            parent, leaf = "", p
        by.setdefault(parent, []).append(leaf)
    out = []
    for parent, leaves in by.items():
        if not parent:
            out += ["use %s;" % l for l in leaves]
        elif len(leaves) == 1:
            out.append("use %s::%s;" % (parent, leaves[0]))
        else:
            out.append("use %s::{%s};" % (parent, ", ".join(leaves)))
    return out

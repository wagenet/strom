#!/usr/bin/env python3
"""Split a Rust source file into a directory module, driven by a manifest.

Usage: split.py MANIFEST [--repo DIR]

Text generation only. It moves items verbatim; the caller then runs
`fixup` (below) to let the compiler decide visibility and prune imports.

Manifest format (one directive per line, '#' comments):

    source backend/src/foo.rs
    module bar //! One-line module doc for bar.rs
    module test_support #[cfg(test)] //! Doc for test_support.rs
    /fn some_fn = bar
    impl Foo/fn method = bar
    impl Foo = mod            # which module keeps the impl's own comments
    tests/fn some_test = bar  # test items; test_support takes fixtures
    replace bar old text => new text   # also a repo path instead of a module

Every item of the source must be listed and every listed item must exist,
so a change on the base that adds or removes an item stops the split with
a list of what to place.
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import keys  # noqa: E402
import rsitems as R  # noqa: E402


BEGIN = "// split:test-imports-begin"
END = "// split:test-imports-end"


def parse_manifest(path):
    specs = []
    cur = None
    for raw in open(path):
        line = raw.rstrip("\n")
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if line.startswith("source "):
            cur = {"source": line.split(None, 1)[1].strip(), "modules": [], "place": {},
                   "impl_owner": {}, "replace": [], "cfg": {}}
            specs.append(cur)
        elif line.startswith("module "):
            m = re.match(r"module (\w+)\s+(#\[[^\]]*\]\s+)?(//!.*)$", line)
            cur["modules"].append((m.group(1), m.group(3)))
            if m.group(2):
                cur["cfg"][m.group(1)] = m.group(2).strip()
        elif line.startswith("replace "):
            target, rest = line.split(None, 2)[1:]
            old, new = rest.split(" => ")
            cur["replace"].append((target, old, new))
        else:
            key, mod = [s.strip() for s in line.rsplit(" = ", 1)]
            if "/" not in key:
                cur["impl_owner"][key] = mod
            else:
                if key in cur["place"]:
                    sys.exit("duplicate manifest key: " + key)
                cur["place"][key] = mod
    return specs


def indent_block(text, n):
    pad = " " * n
    return "".join(pad + l if l.strip() else l for l in text.splitlines(True))


def dedent_block(text, n):
    out = []
    for l in text.splitlines(True):
        if l.strip():
            assert l.startswith(" " * n), l
            l = l[n:]
        out.append(l)
    return "".join(out)


def strip_comments_strings(src):
    out = []
    i, n = 0, len(src)
    while i < n:
        if src.startswith("//", i):
            j = src.find("\n", i)
            i = n if j < 0 else j
            continue
        if src.startswith("/*", i):
            i = R._skip_block_comment(src, i)
            continue
        if src[i] == '"' :
            j = R._skip_string(src, i)
            # keep format-args identifiers: "{name}" references a binding
            out.append(" ".join(re.findall(r"\{(\w+)", src[i:j])) + " ")
            i = j
            continue
        out.append(src[i])
        i += 1
    return "".join(out)


def idents(src):
    code = strip_comments_strings(src)
    return set(re.findall(r"(?<![\w.])\b([A-Za-z_]\w*)\b", code))


def defined_name(key):
    kind, name = key.split("/", 1)[1].split(" ", 1)
    if kind in ("fn", "struct", "enum", "const", "static", "type", "trait", "union", "macro_rules!"):
        return name
    return None


def split(spec, repo):
    src_path = os.path.join(repo, spec["source"])
    src = open(src_path).read()
    modules = [m for m, _ in spec["modules"]]
    docs = dict(spec["modules"])
    if "mod" not in modules:
        sys.exit("manifest must declare `module mod`")

    # File-level //! doc stays with mod.rs.
    m = re.match(r"((?://!.*\n)+)\n*", src)
    header = m.group(1) if m else ""
    body = src[m.end():] if m else src

    # Absolute path of the source's parent module, e.g. `crate::blocks::builtin`.
    rel = spec["source"].split("src/", 1)[1]
    parent_path = "::".join(["crate"] + os.path.dirname(rel).split("/")).rstrip(":")

    items = keys.keyed(body)
    uses, test_uses = [], []
    placed = {mod: [] for mod in modules}  # (container, text, key)
    impl_headers = {}
    seen = set()
    # Re-split impl blocks to recover each header and the comments before it.
    for kind, name, text in R.split_items(body):
        if kind == "impl" and keys.has_body(kind, text):
            o, _ = R.body_span(text)
            impl_headers["impl " + name] = text[: o + 1]
        if kind == "mod" and name == "tests":
            o, _ = R.body_span(text)
            impl_headers["tests"] = text[: o + 1]
    unknown = []
    for cont, key, text, kind in items:
        if key is None:
            if text.strip():
                sys.exit("stray comment block not attached to an item:\n" + text)
            continue
        if key == "use":
            if cont == "tests":
                test_uses.append(text.strip())
            else:
                # A file-level `use super::` names the source's parent, which
                # the new submodules are one level further from.
                uses.append(re.sub(r"^use super::", "use %s::" % parent_path, text.strip()))
            continue
        full = cont + "/" + key
        if full not in spec["place"]:
            unknown.append(full)
            continue
        seen.add(full)
        placed[spec["place"][full]].append((cont, text, full))
    if unknown:
        sys.exit("%s: not in manifest:\n  %s" % (spec["source"], "\n  ".join(unknown)))
    # The manifest may cover items from open PRs this base does not have.
    for key in sorted(set(spec["place"]) - seen):
        print("note: not in this source, skipped: " + key, file=sys.stderr)

    # Names defined at top level of each module, for sibling imports.
    defs = {mod: set() for mod in modules}
    for mod in modules:
        for cont, text, full in placed[mod]:
            n = defined_name(full)
            if n and n != "_" and (cont == "" or mod == "test_support"):
                defs[mod].add(n)

    def render_items(mod):
        top, by_impl, tests = [], {}, []
        order = []
        for cont, text, full in placed[mod]:
            if cont == "":
                top.append(text)
                order.append(("item", len(top) - 1))
            elif cont == "tests":
                if mod == "test_support":
                    top.append(dedent_block(text, 4))
                    order.append(("item", len(top) - 1))
                else:
                    tests.append(text)
            else:
                if cont not in by_impl:
                    by_impl[cont] = []
                    order.append(("impl", cont))
                by_impl[cont].append(text)
        chunks = []
        for what, ref in order:
            if what == "item":
                chunks.append(top[ref])
            else:
                owner = spec["impl_owner"].get(ref, "mod")
                head = impl_headers[ref]
                if owner != mod:
                    head = "\n" + head.strip().splitlines()[-1]
                chunks.append(head + "\n" + "".join(by_impl[ref]).rstrip("\n").lstrip("\n") + "\n}\n")
        return chunks, tests

    rendered = {mod: render_items(mod) for mod in modules}

    # pub items leaving mod.rs keep their path through a re-export.
    reexports, reexported = {}, set()
    for sub in modules:
        if sub in ("mod", "test_support"):
            continue
        for cont, text, full in placed[sub]:
            n = defined_name(full)
            if cont != "" or not n or n == "_":
                continue
            vis = re.match(r"(?:\s*(?://.*|#\[.*)?\n)*\s*(pub(?:\([^)]*\))?)\s", text)
            if vis:
                reexports.setdefault((vis.group(1), sub), []).append(n)
                reexported.add(n)
    reexports = ["%s use %s::{%s};" % (v, sub, ", ".join(ns)) for (v, sub), ns in reexports.items()]

    def sibling_uses(mod, code, from_tests, covered=frozenset()):
        refs = idents(code) - covered
        up = "super::super" if from_tests else "super"
        groups = {}
        for other in modules:
            if other == mod or other == "test_support" and not from_tests:
                continue
            for n in sorted(defs[other] & refs):
                if mod == "mod":
                    if n in reexported and not from_tests:
                        continue
                    base = ("super::" if from_tests else "") + other
                elif other == "mod" or n in reexported:
                    # a sibling's pub item is reached through mod.rs's re-export
                    base = up
                else:
                    base = up + "::" + other
                groups.setdefault(base, []).append(n)
        return ["use %s::{%s};" % (base, ", ".join(ns)) for base, ns in groups.items()]

    out_dir = os.path.splitext(src_path)[0]
    has_support = "test_support" in modules
    files = {}
    for mod in modules:
        chunks, tests = rendered[mod]
        code = "".join(chunks)
        parts = []
        if mod == "mod":
            parts.append(header)
            decls = []
            for sub in sorted(x for x in modules if x != "mod"):
                cfg = spec["cfg"].get(sub)
                decls.append((cfg + "\n" if cfg else "") + "mod %s;" % sub)
            parts.append("\n" + "\n".join(decls) + "\n")
        else:
            parts.append(docs[mod] + "\n")
        u = list(uses)
        if mod == "test_support":
            # Its `use super::*` reaches mod.rs; fixup.py narrows the rest.
            u = test_uses + [BEGIN] + [x for x in u if not x.startswith("use super")] + [END]
        parts.append("\n" + "\n".join(u) + "\n")
        sib = sibling_uses(mod, code, False)
        if mod == "test_support":
            sib = [s for s in sib if not s.startswith("use super::{")]
        if sib:
            parts.append("\n" + "\n".join(sib) + "\n")
        if mod == "mod" and reexports:
            parts.append("\n" + "\n".join(reexports) + "\n")
        parts.append("\n" + "".join(chunks).strip("\n") + "\n" if chunks else "")
        if tests:
            tbody = "".join(tests)
            tu = list(test_uses)
            # Names the module's own code already uses reach the tests through
            # `use super::*`.
            covered = idents(code) | (reexported if mod == "mod" else set())
            tu += sibling_uses(mod, tbody, True, covered)
            # fixup.py keeps only the file-level imports the module itself
            # no longer has; the rest reach the tests through `use super::*`.
            tu += [BEGIN] + [x for x in uses if not x.startswith("use super")] + [END]
            head = impl_headers["tests"]
            parts.append("\n" + head.rstrip() + "\n" + indent_block("\n".join(tu) + "\n", 4)
                         + "\n" + tbody.lstrip("\n").rstrip("\n") + "\n}\n")
        text = "".join(parts)
        for rmod, old, new in spec["replace"]:
            if rmod == mod:
                if old not in text:
                    sys.exit("replace target not found in %s: %s" % (mod, old))
                text = text.replace(old, new)
        path = os.path.join(out_dir, mod + ".rs")
        files[path] = text
    for rmod, old, new in spec["replace"]:
        if "/" in rmod:
            path = os.path.join(repo, rmod)
            text = open(path).read()
            if old not in text:
                sys.exit("replace target not found in %s: %s" % (rmod, old))
            files[path] = text.replace(old, new)
    return src_path, files


def main():
    args = sys.argv[1:]
    repo = "."
    if "--repo" in args:
        i = args.index("--repo")
        repo = args[i + 1]
        del args[i:i + 2]
    # Check every source against the manifest before writing anything.
    results = [split(spec, repo) for spec in parse_manifest(args[0])]
    for src_path, files in results:
        for path, text in files.items():
            os.makedirs(os.path.dirname(path), exist_ok=True)
            open(path, "w").write(text)
            print(path)
        os.remove(src_path)


if __name__ == "__main__":
    main()

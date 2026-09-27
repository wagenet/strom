#!/usr/bin/env python3
"""Make split.py's output compile, using only the compiler's verdicts.

1. Raise to `pub(super)` exactly the items and fields the compiler reports
   as private at a use site in another new module, until none remain.
2. `cargo fix` removes the imports split.py over-supplied.
3. `cargo fmt`.

Usage: fixup.py FILE...   (the generated files; run from the workspace root)
"""

import json
import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import rsitems  # noqa: E402
import split  # noqa: E402
import usetree  # noqa: E402

PRIVATE = re.compile(
    r"^(?:function|constant|struct|enum|static|type alias|unit struct|tuple struct|"
    r"method|associated function|associated constant|trait) `(\w+)` is private$"
)
FIELD = re.compile(r"^fields? `(\w+)`.* of struct `(\w+)` (?:is|are) private$")
ENV = dict(os.environ, CARGO_PROFILE_DEV_DEBUG="0", CARGO_INCREMENTAL="0")


def check(target="--all-targets"):
    p = subprocess.run(
        ["cargo", "check", target, "--message-format=json"],
        capture_output=True, text=True, env=ENV,
    )
    diags = []
    for line in p.stdout.splitlines():
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        if msg.get("reason") == "compiler-message":
            diags.append(msg["message"])
    return diags


def spans(d):
    out = list(d.get("spans", []))
    for c in d.get("children", []):
        out += spans(c)
    return out


def bump_item(files, name, hint_spans):
    pat = re.compile(
        r"^(\s*)((?:(?:const|async|unsafe)\s+)*(?:fn|const|static|struct|enum|type|trait)\s+%s\b)"
        % re.escape(name)
    )
    hits = []
    for f in files:
        lines = open(f).read().split("\n")
        for i, l in enumerate(lines):
            if pat.match(l):
                hits.append((f, i))
    # Prefer the definition the diagnostic points at.
    pointed = {(os.path.abspath(s["file_name"]), s["line_start"] - 1) for s in hint_spans}
    chosen = [h for h in hits if (os.path.abspath(h[0]), h[1]) in pointed] or hits
    if len(chosen) != 1:
        sys.exit("cannot place private item %s: %r" % (name, chosen))
    f, i = chosen[0]
    lines = open(f).read().split("\n")
    lines[i] = pat.sub(r"\1pub(super) \2", lines[i])
    open(f, "w").write("\n".join(lines))
    return "%s:%d %s" % (f, i + 1, name)


def bump_field(files, field, struct):
    for f in files:
        src = open(f).read()
        m = re.search(r"\bstruct %s\b[^{;]*\{" % re.escape(struct), src)
        if not m:
            continue
        depth, i = 1, m.end()
        while depth:
            depth += {"{": 1, "}": -1}.get(src[i], 0)
            i += 1
        body = src[m.end():i]
        new = re.sub(r"(?m)^(\s*)(%s\s*:)" % re.escape(field), r"\1pub(super) \2", body, count=1)
        if new == body:
            sys.exit("field %s.%s not found" % (struct, field))
        open(f, "w").write(src[: m.end()] + new + src[i:])
        return "%s %s.%s" % (f, struct, field)
    sys.exit("struct %s not found" % struct)


def prune(target, files):
    """Delete every import rustc reports unused in `target`, until none are.

    Not `cargo fix`: rustc offers no suggestion for an unused trait import,
    and those are most of what the split over-supplies."""
    wanted = {os.path.abspath(f) for f in files}
    for _ in range(10):
        cuts = {}
        for d in check(target):
            if d["level"] == "error":
                sys.exit(d.get("rendered") or d["message"])
            if (d.get("code") or {}).get("code") != "unused_imports":
                continue
            for sp in d["spans"]:
                f = os.path.abspath(sp["file_name"])
                if f not in wanted:
                    sys.exit("unused import outside the split: " + d["rendered"])
                cuts.setdefault(f, set()).add((sp["byte_start"], sp["byte_end"]))
        if not cuts:
            return
        for f, spans_ in cuts.items():
            src = open(f, "rb").read()
            for a, b in sorted(spans_, reverse=True):
                src = src[:a] + src[b:]
            open(f, "wb").write(tidy_uses(src.decode()).encode())
    sys.exit("imports still unused after 10 rounds")


def tidy_uses(src):
    """Repair the `use` trees a span deletion left behind."""
    def fix(m):
        t = m.group(0)
        prev = None
        while prev != t:
            prev = t
            # a glob's span stops before its `::*`
            t = re.sub(r"(?<![\w:])::\*", "", t)
            t = re.sub(r"\{\s*,", "{", t)
            t = re.sub(r",\s*,", ",", t)
            t = re.sub(r",\s*\}", "}", t)
            t = re.sub(r"(\w+::)+\{\s*\}", "", t)
            t = re.sub(r"\{\s*\}", "", t)
        if re.fullmatch(r"(pub(\([^)]*\))?\s+)?use\s*(::)?\s*;", t.strip()):
            return ""
        return t
    # rustfmt later folds the blank lines an emptied group leaves.
    return re.sub(r"(?m)^[ \t]*(?:pub(?:\([^)]*\))?\s+)?use\b[^;]*;[ \t]*\n?", fix, src)


def narrow_test_imports(f):
    src = open(f).read()
    b = src.find(split.BEGIN)
    if b < 0:
        return
    e = src.index(split.END)
    # The imports the glob in front of the block already supplies: the
    # module's own for a test module, mod.rs's for test_support.
    if os.path.basename(f) == "test_support.rs":
        outer = open(os.path.join(os.path.dirname(f), "mod.rs")).read()
    else:
        outer = src
    cut = outer.find("mod tests")
    top = set()
    for kind, name, text in rsitems.split_items(outer if cut < 0 else outer[:cut]):
        if kind == "use" and not re.match(r"\s*(//.*\n|\s)*pub", text):
            top.update(usetree.flatten(re.sub(r"(?s)^.*?\buse ", "use ", text, count=1)))
    block = src[b + len(split.BEGIN): e]
    want = []
    for kind, name, text in rsitems.split_items(block):
        if kind == "use":
            want += [p for p in usetree.flatten(text) if p not in top]
    line_start = src.rfind("\n", 0, b) + 1
    indent = src[line_start:b]
    new = ("\n" + indent).join(usetree.group(want))
    end = e + len(split.END)
    if not new:
        # drop the whole marker line
        end = src.index("\n", end) + 1
        b = line_start
        new = ""
    open(f, "w").write(src[:b] + new + src[end:])


def main():
    files = sys.argv[1:]
    bumped = []
    while True:
        diags = check()
        errors = [d for d in diags if d["level"] == "error"]
        todo = []
        for d in errors:
            m = PRIVATE.match(d["message"])
            if m:
                todo.append(("item", m.group(1), spans(d)))
                continue
            m = FIELD.match(d["message"])
            if m:
                todo.append(("field", m.group(1), m.group(2)))
                continue
        if not todo:
            if errors:
                for d in errors:
                    sys.stderr.write(d.get("rendered") or d["message"])
                sys.exit("errors other than privacy remain")
            break
        done = set()
        for t in todo:
            if t[:2] in done:
                continue
            done.add(t[:2])
            if t[0] == "item":
                bumped.append(bump_item(files, t[1], t[2]))
            else:
                bumped.append(bump_field(files, t[1], t[2]))
    for b in bumped:
        print("pub(super):", b)
    # Prune imports against the non-test code first, so an import both use
    # stays at the top of the module and reaches the tests through
    # `use super::*`.
    prune("--lib", files)
    for f in files:
        narrow_test_imports(f)
    prune("--all-targets", files)
    # Fixture imports were explicit so the compiler would name each private
    # one; the tests take them by glob.
    for f in files:
        src = open(f).read()
        out = re.sub(r"(use (?:super::)+test_support::)(?:\{[^}]*\}|\w+);", r"\1*;", src)
        if out != src:
            open(f, "w").write(out)
    subprocess.run(["cargo", "fmt", "--all"], check=True)
    leftover = [d for d in check() if d["level"] in ("error", "warning")]
    for d in leftover:
        sys.stderr.write(d.get("rendered") or d["message"])
    if leftover:
        sys.exit("diagnostics remain after cargo fix")


if __name__ == "__main__":
    main()

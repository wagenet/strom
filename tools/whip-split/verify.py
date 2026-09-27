#!/usr/bin/env python3
"""Check that a split is a move: compare the multiset of code lines.

Usage: verify.py BASE_REV [HEAD_REV]   (run in the repo; HEAD_REV defaults
to the working tree)

`use` declarations are left out (the split re-derives them), leading
whitespace is ignored (test and impl bodies change indentation), and a
leading `pub(super) ` is ignored. What remains should differ only by
module scaffolding, which is printed for review.
"""
import collections
import re
import subprocess
import sys

OLD = ["backend/src/blocks/builtin/whip.rs", "backend/src/whip_session_manager.rs"]
NEW = ["backend/src/blocks/builtin/whip", "backend/src/whip_session_manager"]


def git(*a):
    return subprocess.run(["git", *a], capture_output=True, text=True, check=True).stdout


def lines(text):
    out, in_use = [], False
    for l in text.splitlines():
        t = l.strip()
        if not t:
            continue
        if re.match(r"(pub(\([^)]*\))? )?use\b", t):
            in_use = not t.endswith(";")
            continue
        if in_use:
            in_use = not t.endswith(";")
            continue
        out.append(re.sub(r"^pub\(super\) ", "", t))
    return out


def read(rev, path):
    if rev is None:
        return open(path).read()
    return git("show", "%s:%s" % (rev, path))


def files(rev, d):
    if rev is None:
        return sorted(git("ls-files", "--others", "--cached", "--exclude-standard", d).split())
    return sorted(git("ls-tree", "-r", "--name-only", rev, d).split())


base = sys.argv[1]
head = sys.argv[2] if len(sys.argv) > 2 else None
old = collections.Counter()
for p in OLD:
    old.update(lines(read(base, p)))
new = collections.Counter()
for d in NEW:
    for p in files(head, d):
        new.update(lines(read(head, p)))
gone = old - new
added = new - old
print("in the old files only (%d):" % sum(gone.values()))
for l, n in sorted(gone.items()):
    print("  %dx %s" % (n, l))
print("in the new files only (%d):" % sum(added.values()))
for l, n in sorted(added.items()):
    print("  %dx %s" % (n, l))

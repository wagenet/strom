# whip-split

Regenerates the split of `backend/src/blocks/builtin/whip.rs` and
`backend/src/whip_session_manager.rs` into directory modules (Eyevinn/strom
PR 854) on any base, instead of hand-merging the split against other WHIP
changes.

Run from the root of the checkout to split, with this directory anywhere:

```sh
T=/path/to/tools/whip-split
python3 $T/split.py $T/whip.manifest
python3 $T/fixup.py backend/src/blocks/builtin/whip/*.rs backend/src/whip_session_manager/*.rs
python3 $T/verify.py HEAD      # after `git add -A`, before committing
```

- `split.py` places every item of the two files by `whip.manifest` and moves
  its text unchanged. It stops if a file has an item the manifest does not
  place; add a line for it and rerun. Items the base lacks are skipped.
- `fixup.py` makes the result compile using only compiler output: it raises
  to `pub(super)` exactly the items reported private at a use site, then
  deletes the imports reported unused, then runs `cargo fmt`. It builds with
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- `verify.py` compares the code lines of the old files with the new ones,
  ignoring imports, indentation and `pub(super)`. What remains should be
  module scaffolding and the path comments the manifest rewrites.

The same manifest regenerates PR 854 on upstream/main and the
integration branch on top of every open WHIP PR.

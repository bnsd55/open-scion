A checkout now refuses a tree entry whose name turns into `.git` or
`.scion` once the parts a disk folds are gone: trailing ASCII spaces and
dots (NTFS drops them) and U+200C..U+200F anywhere (HFS+ ignores them).
Before, a malicious server's tree could smuggle a `.git ` directory past
the reserved-name check into a checkout.

Case-insensitive: `fix/reserved-name-trim` off `origin/main`.

Tested on: macOS 15 (APFS)

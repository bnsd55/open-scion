A checkout now refuses a tree entry whose name turns into `.git` or
`.scion` once the parts a disk folds are gone: trailing ASCII spaces and
dots (NTFS drops them) and the code points HFS+ ignores anywhere
(U+200C..U+200F, U+202A..U+202E, U+206A..U+206F, U+FEFF). Before, a
malicious server's tree could smuggle a `.git ` directory past the
reserved-name check into a checkout.

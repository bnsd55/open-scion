The docs that walk an operator through making the files the server reads
said nothing about the file modes the reads now check: the README's
access-file section showed the format but not the mode, the README's and
operations' TLS sections told the operator to make certificates with a
tool and started the server on them, and the Postgres section named the
CA file without its rule. An operator following them under umask 002 —
a default Ubuntu account — wrote an access file or a certificate the
server then refused at start. Each place now states the rule where the
file is first made, with the fix: a secret (the access file, the TLS
private keys) is read when its owner alone can read it, chmod 600; a
file that must stay whole (the TLS certificate, the CAs of --tls-ca,
--tls-client-ca and --postgres-ca) must not be group- or others-
writable, chmod go-w, and a copy tool or a umask-002 session can leave
0664 behind.

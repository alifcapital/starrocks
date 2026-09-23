StringZilla UTF-8 case conversion
================================

Source: https://github.com/alifcapital/StringZilla
Commit: 1647acc8e92a28972de864c7e3771b7538374f42
Branch: feature/utf8-case-unicode17 (upstream v5.1.2 plus Unicode 17 lower/upper/INITCAP).

The UTF-8 case headers and their dependencies are copied unchanged from this commit.
Runtime backend selection is in util/utf8_case.cpp. SQL LOWER/UPPER use full default
Unicode mappings; ngram comparison uses the same lowercase mappings. No locale tailoring or
normalization is applied. LOWER/UPPER preserve invalid UTF-8 bytes.
INITCAP uses simple mappings over runs of Unicode letters and decimal digits,
with the first character uppercase and the rest lowercase. Invalid UTF-8 is rejected
with the sequence offset. It does not use folding or full titlecase mappings.

Other legacy headers in this directory and ../stringzillas are not used by case
conversion; they are retained for the separate string similarity branches.

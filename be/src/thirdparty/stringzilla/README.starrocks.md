StringZilla UTF-8 case conversion
================================

Source: https://github.com/alifcapital/StringZilla
Commit: 29f7e4663b8883c29c40c2c5308a260b5f3f7371
Branch: feature/utf8-case-unicode17 (upstream v5.1.2 plus Unicode 17 lower/upper).

The UTF-8 case headers and their dependencies are copied unchanged from this commit.
Runtime backend selection is in util/utf8_case.cpp. SQL LOWER/UPPER use full default
Unicode mappings; ngram comparison uses full case folding. No locale tailoring or
normalization is applied. Invalid UTF-8 bytes are preserved.

Other legacy headers in this directory and ../stringzillas are not used by case
conversion; they are retained for the separate string similarity branches.

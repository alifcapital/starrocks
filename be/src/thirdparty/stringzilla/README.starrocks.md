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

The StringZillas CPU similarity headers and their StringZilla dependencies are
also copied from the same pinned commit for the name-similarity functions.
ForkUnion's required header is pinned to the upstream submodule revision
7f52520ea00a6bf8e7a1f46348dad345ba361096; its license is ../LICENSE.forkunion.
The SQL integration uses the serial executor, not a ForkUnion thread pool.
Similarity backends follow the compiled ISA: AVX-512, AVX2 or scalar. UTF-8
inputs are validated before the similarity engine's unchecked decoding.
Tajik weighted distance keeps its separate character-dependent cost model.
Remaining legacy headers are not used by these paths.

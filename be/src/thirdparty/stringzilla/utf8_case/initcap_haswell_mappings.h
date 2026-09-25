/* Generated Unicode 17 simple INITCAP transforms. See UNICODE-LICENSE.txt. */
#ifndef STRINGZILLA_UTF8_INITCAP_HASWELL_MAPPINGS_H_
#define STRINGZILLA_UTF8_INITCAP_HASWELL_MAPPINGS_H_
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_haswell_greek_basic_(__m256i v, sz_ptr_t target, sz_bool_t *word_start) {
    __m256i prev1 = sz_haswell_previous_bytes_(v, 1);
    __m256i continuations = sz_haswell_in_byte_range_(v, 0x80, 0x40);
    sz_u32_t allowed = ~(sz_u32_t)_mm256_movemask_epi8(v);
    sz_u32_t alnum = (sz_u32_t)_mm256_movemask_epi8(
        _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26)),
                        sz_haswell_in_byte_range_(v, 48, 10)));
    sz_u32_t stop = 0;
    __m256i pce = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)206)), continuations);
    sz_u32_t pce_bits = (sz_u32_t)_mm256_movemask_epi8(pce);
    allowed |= (pce_bits >> 0) | (pce_bits >> 1);
    sz_u32_t pce_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pce,
        _mm256_or_si256(
            _mm256_or_si256(
                _mm256_or_si256(
                    _mm256_or_si256(_mm256_and_si256(sz_haswell_in_byte_range_(v, 134, 3),
                                                     _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                       _mm256_set1_epi8(0))),
                                    sz_haswell_in_byte_range_(v, 137, 2)),
                    _mm256_and_si256(sz_haswell_in_byte_range_(v, 140, 3),
                                     _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)), _mm256_set1_epi8(0)))),
                sz_haswell_in_byte_range_(v, 143, 19)),
            sz_haswell_in_byte_range_(v, 163, 29))));
    alnum |= (pce_word >> 0) | (pce_word >> 1);
    __m256i pcf = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)207)), continuations);
    sz_u32_t pcf_bits = (sz_u32_t)_mm256_movemask_epi8(pcf);
    allowed |= (pcf_bits >> 0) | (pcf_bits >> 1);
    sz_u32_t pcf_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pcf, _mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 54), sz_haswell_in_byte_range_(v, 183, 9))));
    alnum |= (pcf_word >> 0) | (pcf_word >> 1);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u32(stop) : 32;
    if (!length) return 0;
    sz_u32_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    __m256i capitalize = sz_utf8_initcap_haswell_expand_mask_(starts);
    __m256i letters = _mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26));
    __m256i result = _mm256_or_si256(v, _mm256_and_si256(letters, _mm256_set1_epi8(32)));
    result = _mm256_sub_epi8(result, _mm256_and_si256(_mm256_and_si256(capitalize, letters), _mm256_set1_epi8(32)));
    __m256i match0 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pce, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)134))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match0, _mm256_set1_epi8((char)38)));
    __m256i match1 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pce, sz_haswell_in_byte_range_(v, 136, 3)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match1, _mm256_set1_epi8((char)37)));
    __m256i match2 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pce, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)140))));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match2), _mm256_set1_epi8((char)1)));
    __m256i match3 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pce, sz_haswell_in_byte_range_(v, 142, 2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match3, _mm256_set1_epi8((char)255)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match3), _mm256_set1_epi8((char)1)));
    __m256i match4 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pce, sz_haswell_in_byte_range_(v, 145, 15)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match4, _mm256_set1_epi8((char)32)));
    __m256i match5 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pce, _mm256_or_si256(sz_haswell_in_byte_range_(v, 160, 2),
                                                                               sz_haswell_in_byte_range_(v, 163, 9))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match5, _mm256_set1_epi8((char)224)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match5), _mm256_set1_epi8((char)1)));
    __m256i match6 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pce, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)172))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match6, _mm256_set1_epi8((char)218)));
    __m256i match7 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pce, sz_haswell_in_byte_range_(v, 173, 3)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match7, _mm256_set1_epi8((char)219)));
    __m256i match8 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pce, sz_haswell_in_byte_range_(v, 177, 15)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match8, _mm256_set1_epi8((char)224)));
    __m256i match9 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pcf, _mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 2),
                                                                            sz_haswell_in_byte_range_(v, 131, 9))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match9, _mm256_set1_epi8((char)32)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match9), _mm256_set1_epi8((char)255)));
    __m256i match10 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)130))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match10, _mm256_set1_epi8((char)33)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match10), _mm256_set1_epi8((char)255)));
    __m256i match11 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)140))));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match11), _mm256_set1_epi8((char)255)));
    __m256i match12 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, sz_haswell_in_byte_range_(v, 141, 2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match12, _mm256_set1_epi8((char)1)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match12), _mm256_set1_epi8((char)255)));
    __m256i match13 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)143))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match13, _mm256_set1_epi8((char)8)));
    __m256i match14 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)144))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match14, _mm256_set1_epi8((char)2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match14), _mm256_set1_epi8((char)255)));
    __m256i match15 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)145))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match15, _mm256_set1_epi8((char)7)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match15), _mm256_set1_epi8((char)255)));
    __m256i match16 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)149))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match16, _mm256_set1_epi8((char)17)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match16), _mm256_set1_epi8((char)255)));
    __m256i match17 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)150))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match17, _mm256_set1_epi8((char)10)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match17), _mm256_set1_epi8((char)255)));
    __m256i match18 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)151))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match18, _mm256_set1_epi8((char)248)));
    __m256i match19 = _mm256_andnot_si256(
        sz_haswell_previous_bytes_(capitalize, 1),
        _mm256_and_si256(
            pcf,
            _mm256_or_si256(_mm256_or_si256(_mm256_and_si256(sz_haswell_in_byte_range_(v, 152, 23),
                                                             _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                               _mm256_set1_epi8(0))),
                                            _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)183))),
                            _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)186)))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match19, _mm256_set1_epi8((char)1)));
    __m256i match20 = _mm256_and_si256(
        sz_haswell_previous_bytes_(capitalize, 1),
        _mm256_and_si256(
            pcf,
            _mm256_or_si256(_mm256_or_si256(_mm256_and_si256(sz_haswell_in_byte_range_(v, 153, 23),
                                                             _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                               _mm256_set1_epi8(1))),
                                            _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)184))),
                            _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)187)))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match20, _mm256_set1_epi8((char)255)));
    __m256i match21 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)176))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match21, _mm256_set1_epi8((char)234)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match21), _mm256_set1_epi8((char)255)));
    __m256i match22 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)177))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match22, _mm256_set1_epi8((char)240)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match22), _mm256_set1_epi8((char)255)));
    __m256i match23 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)178))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match23, _mm256_set1_epi8((char)7)));
    __m256i match24 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)179))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match24, _mm256_set1_epi8((char)12)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match24), _mm256_set1_epi8((char)254)));
    __m256i match25 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)180))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match25, _mm256_set1_epi8((char)4)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match25), _mm256_set1_epi8((char)255)));
    __m256i match26 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)181))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match26, _mm256_set1_epi8((char)224)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match26), _mm256_set1_epi8((char)255)));
    __m256i match27 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)185))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match27, _mm256_set1_epi8((char)249)));
    __m256i match28 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pcf, sz_haswell_in_byte_range_(v, 189, 3)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match28, _mm256_set1_epi8((char)254)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match28), _mm256_set1_epi8((char)254)));
    _mm256_storeu_si256((__m256i *)target, result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_haswell_latin1_(__m256i v, sz_ptr_t target, sz_bool_t *word_start) {
    __m256i prev1 = sz_haswell_previous_bytes_(v, 1);
    __m256i continuations = sz_haswell_in_byte_range_(v, 0x80, 0x40);
    sz_u32_t allowed = ~(sz_u32_t)_mm256_movemask_epi8(v);
    sz_u32_t alnum = (sz_u32_t)_mm256_movemask_epi8(
        _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26)),
                        sz_haswell_in_byte_range_(v, 48, 10)));
    sz_u32_t stop = 0;
    __m256i pc2 = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)194)), continuations);
    sz_u32_t pc2_bits = (sz_u32_t)_mm256_movemask_epi8(pc2);
    allowed |= (pc2_bits >> 0) | (pc2_bits >> 1);
    sz_u32_t pc2_word = (sz_u32_t)_mm256_movemask_epi8(
        _mm256_and_si256(pc2, _mm256_or_si256(_mm256_or_si256(_mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)170)),
                                                              _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)181))),
                                              _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)186)))));
    alnum |= (pc2_word >> 0) | (pc2_word >> 1);
    __m256i pc3 = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)195)), continuations);
    sz_u32_t pc3_bits = (sz_u32_t)_mm256_movemask_epi8(pc3);
    allowed |= (pc3_bits >> 0) | (pc3_bits >> 1);
    sz_u32_t pc3_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pc3,
        _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 23), sz_haswell_in_byte_range_(v, 152, 31)),
                        sz_haswell_in_byte_range_(v, 184, 8))));
    alnum |= (pc3_word >> 0) | (pc3_word >> 1);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u32(stop) : 32;
    if (!length) return 0;
    sz_u32_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    __m256i capitalize = sz_utf8_initcap_haswell_expand_mask_(starts);
    __m256i letters = _mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26));
    __m256i result = _mm256_or_si256(v, _mm256_and_si256(letters, _mm256_set1_epi8(32)));
    result = _mm256_sub_epi8(result, _mm256_and_si256(_mm256_and_si256(capitalize, letters), _mm256_set1_epi8(32)));
    __m256i match0 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pc2, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)181))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match0, _mm256_set1_epi8((char)231)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match0), _mm256_set1_epi8((char)12)));
    __m256i match1 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pc3, _mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 23),
                                                                               sz_haswell_in_byte_range_(v, 152, 7))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match1, _mm256_set1_epi8((char)32)));
    __m256i match2 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pc3, _mm256_or_si256(sz_haswell_in_byte_range_(v, 160, 23),
                                                                            sz_haswell_in_byte_range_(v, 184, 7))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match2, _mm256_set1_epi8((char)224)));
    __m256i match3 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pc3, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)191))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match3, _mm256_set1_epi8((char)249)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match3), _mm256_set1_epi8((char)2)));
    _mm256_storeu_si256((__m256i *)target, result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_haswell_latin_(__m256i v, sz_ptr_t target, sz_bool_t *word_start) {
    __m256i prev1 = sz_haswell_previous_bytes_(v, 1);
    __m256i prev2 = sz_haswell_previous_bytes_(v, 2);
    __m256i continuations = sz_haswell_in_byte_range_(v, 0x80, 0x40);
    sz_u32_t allowed = ~(sz_u32_t)_mm256_movemask_epi8(v);
    sz_u32_t alnum = (sz_u32_t)_mm256_movemask_epi8(
        _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26)),
                        sz_haswell_in_byte_range_(v, 48, 10)));
    sz_u32_t stop = 0;
    __m256i pc2 = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)194)), continuations);
    sz_u32_t pc2_bits = (sz_u32_t)_mm256_movemask_epi8(pc2);
    allowed |= (pc2_bits >> 0) | (pc2_bits >> 1);
    sz_u32_t pc2_word = (sz_u32_t)_mm256_movemask_epi8(
        _mm256_and_si256(pc2, _mm256_or_si256(_mm256_or_si256(_mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)170)),
                                                              _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)181))),
                                              _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)186)))));
    alnum |= (pc2_word >> 0) | (pc2_word >> 1);
    __m256i pc3 = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)195)), continuations);
    sz_u32_t pc3_bits = (sz_u32_t)_mm256_movemask_epi8(pc3);
    allowed |= (pc3_bits >> 0) | (pc3_bits >> 1);
    sz_u32_t pc3_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pc3,
        _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 23), sz_haswell_in_byte_range_(v, 152, 31)),
                        sz_haswell_in_byte_range_(v, 184, 8))));
    alnum |= (pc3_word >> 0) | (pc3_word >> 1);
    __m256i pc4 = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)196)), continuations);
    sz_u32_t pc4_bits = (sz_u32_t)_mm256_movemask_epi8(pc4);
    allowed |= (pc4_bits >> 0) | (pc4_bits >> 1);
    sz_u32_t pc4_word = (sz_u32_t)_mm256_movemask_epi8(pc4);
    alnum |= (pc4_word >> 0) | (pc4_word >> 1);
    stop |= ((sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(pc4, sz_haswell_in_byte_range_(v, 176, 2)))) >> 1;
    __m256i pc5 = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)197)), continuations);
    sz_u32_t pc5_bits = (sz_u32_t)_mm256_movemask_epi8(pc5);
    allowed |= (pc5_bits >> 0) | (pc5_bits >> 1);
    sz_u32_t pc5_word = (sz_u32_t)_mm256_movemask_epi8(pc5);
    alnum |= (pc5_word >> 0) | (pc5_word >> 1);
    stop |=
        ((sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(pc5, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)191))))) >> 1;
    __m256i pc6 = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)198)), continuations);
    sz_u32_t pc6_bits = (sz_u32_t)_mm256_movemask_epi8(pc6);
    allowed |= (pc6_bits >> 0) | (pc6_bits >> 1);
    sz_u32_t pc6_word = (sz_u32_t)_mm256_movemask_epi8(pc6);
    alnum |= (pc6_word >> 0) | (pc6_word >> 1);
    stop |=
        ((sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)155))))) >> 1;
    __m256i pe1b8 = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)184)),
                                                      _mm256_cmpeq_epi8(prev2, _mm256_set1_epi8((char)225))),
                                     continuations);
    sz_u32_t pe1b8_bits = (sz_u32_t)_mm256_movemask_epi8(pe1b8);
    allowed |= (pe1b8_bits >> 0) | (pe1b8_bits >> 1) | (pe1b8_bits >> 2);
    sz_u32_t pe1b8_word = (sz_u32_t)_mm256_movemask_epi8(pe1b8);
    alnum |= (pe1b8_word >> 0) | (pe1b8_word >> 1) | (pe1b8_word >> 2);
    __m256i pe1b9 = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)185)),
                                                      _mm256_cmpeq_epi8(prev2, _mm256_set1_epi8((char)225))),
                                     continuations);
    sz_u32_t pe1b9_bits = (sz_u32_t)_mm256_movemask_epi8(pe1b9);
    allowed |= (pe1b9_bits >> 0) | (pe1b9_bits >> 1) | (pe1b9_bits >> 2);
    sz_u32_t pe1b9_word = (sz_u32_t)_mm256_movemask_epi8(pe1b9);
    alnum |= (pe1b9_word >> 0) | (pe1b9_word >> 1) | (pe1b9_word >> 2);
    __m256i pe1ba = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)186)),
                                                      _mm256_cmpeq_epi8(prev2, _mm256_set1_epi8((char)225))),
                                     continuations);
    sz_u32_t pe1ba_bits = (sz_u32_t)_mm256_movemask_epi8(pe1ba);
    allowed |= (pe1ba_bits >> 0) | (pe1ba_bits >> 1) | (pe1ba_bits >> 2);
    sz_u32_t pe1ba_word = (sz_u32_t)_mm256_movemask_epi8(pe1ba);
    alnum |= (pe1ba_word >> 0) | (pe1ba_word >> 1) | (pe1ba_word >> 2);
    stop |=
        ((sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(pe1ba, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)158))))) >>
        2;
    __m256i pe1bb = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)187)),
                                                      _mm256_cmpeq_epi8(prev2, _mm256_set1_epi8((char)225))),
                                     continuations);
    sz_u32_t pe1bb_bits = (sz_u32_t)_mm256_movemask_epi8(pe1bb);
    allowed |= (pe1bb_bits >> 0) | (pe1bb_bits >> 1) | (pe1bb_bits >> 2);
    sz_u32_t pe1bb_word = (sz_u32_t)_mm256_movemask_epi8(pe1bb);
    alnum |= (pe1bb_word >> 0) | (pe1bb_word >> 1) | (pe1bb_word >> 2);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u32(stop) : 32;
    if (!length) return 0;
    sz_u32_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    __m256i capitalize = sz_utf8_initcap_haswell_expand_mask_(starts);
    __m256i letters = _mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26));
    __m256i result = _mm256_or_si256(v, _mm256_and_si256(letters, _mm256_set1_epi8(32)));
    result = _mm256_sub_epi8(result, _mm256_and_si256(_mm256_and_si256(capitalize, letters), _mm256_set1_epi8(32)));
    __m256i match0 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pc2, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)181))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match0, _mm256_set1_epi8((char)231)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match0), _mm256_set1_epi8((char)12)));
    __m256i match1 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pc3, _mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 23),
                                                                               sz_haswell_in_byte_range_(v, 152, 7))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match1, _mm256_set1_epi8((char)32)));
    __m256i match2 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pc3, _mm256_or_si256(sz_haswell_in_byte_range_(v, 160, 23),
                                                                            sz_haswell_in_byte_range_(v, 184, 7))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match2, _mm256_set1_epi8((char)224)));
    __m256i match3 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pc3, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)191))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match3, _mm256_set1_epi8((char)249)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match3), _mm256_set1_epi8((char)2)));
    __m256i match4 = _mm256_andnot_si256(
        sz_haswell_previous_bytes_(capitalize, 1),
        _mm256_or_si256(
            _mm256_or_si256(
                _mm256_and_si256(
                    pc4,
                    _mm256_or_si256(
                        _mm256_or_si256(_mm256_and_si256(sz_haswell_in_byte_range_(v, 128, 47),
                                                         _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                           _mm256_set1_epi8(0))),
                                        _mm256_and_si256(sz_haswell_in_byte_range_(v, 178, 5),
                                                         _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                           _mm256_set1_epi8(0)))),
                        _mm256_and_si256(
                            sz_haswell_in_byte_range_(v, 185, 5),
                            _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)), _mm256_set1_epi8(1))))),
                _mm256_and_si256(
                    pc5,
                    _mm256_or_si256(
                        _mm256_or_si256(_mm256_and_si256(sz_haswell_in_byte_range_(v, 129, 7),
                                                         _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                           _mm256_set1_epi8(1))),
                                        _mm256_and_si256(sz_haswell_in_byte_range_(v, 138, 45),
                                                         _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                           _mm256_set1_epi8(0)))),
                        _mm256_and_si256(
                            sz_haswell_in_byte_range_(v, 185, 5),
                            _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)), _mm256_set1_epi8(1)))))),
            _mm256_and_si256(
                pc6,
                _mm256_or_si256(
                    _mm256_or_si256(
                        _mm256_or_si256(
                            _mm256_or_si256(
                                _mm256_or_si256(
                                    _mm256_or_si256(
                                        _mm256_or_si256(
                                            _mm256_or_si256(
                                                _mm256_or_si256(
                                                    _mm256_or_si256(
                                                        _mm256_or_si256(
                                                            _mm256_and_si256(
                                                                sz_haswell_in_byte_range_(v, 130, 3),
                                                                _mm256_cmpeq_epi8(
                                                                    _mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                    _mm256_set1_epi8(0))),
                                                            _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)135))),
                                                        _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)139))),
                                                    _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)145))),
                                                _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)152))),
                                            _mm256_and_si256(sz_haswell_in_byte_range_(v, 160, 5),
                                                             _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                               _mm256_set1_epi8(0)))),
                                        _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)167))),
                                    _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)172))),
                                _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)175))),
                            _mm256_and_si256(
                                sz_haswell_in_byte_range_(v, 179, 3),
                                _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)), _mm256_set1_epi8(1)))),
                        _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)184))),
                    _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)188))))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match4, _mm256_set1_epi8((char)1)));
    __m256i match5 = _mm256_and_si256(
        sz_haswell_previous_bytes_(capitalize, 1),
        _mm256_or_si256(
            _mm256_or_si256(
                _mm256_and_si256(
                    pc4,
                    _mm256_or_si256(
                        _mm256_or_si256(_mm256_and_si256(sz_haswell_in_byte_range_(v, 129, 47),
                                                         _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                           _mm256_set1_epi8(1))),
                                        _mm256_and_si256(sz_haswell_in_byte_range_(v, 179, 5),
                                                         _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                           _mm256_set1_epi8(1)))),
                        _mm256_and_si256(
                            sz_haswell_in_byte_range_(v, 186, 5),
                            _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)), _mm256_set1_epi8(0))))),
                _mm256_and_si256(
                    pc5,
                    _mm256_or_si256(
                        _mm256_or_si256(_mm256_and_si256(sz_haswell_in_byte_range_(v, 130, 7),
                                                         _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                           _mm256_set1_epi8(0))),
                                        _mm256_and_si256(sz_haswell_in_byte_range_(v, 139, 45),
                                                         _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                           _mm256_set1_epi8(1)))),
                        _mm256_and_si256(
                            sz_haswell_in_byte_range_(v, 186, 5),
                            _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)), _mm256_set1_epi8(0)))))),
            _mm256_and_si256(
                pc6,
                _mm256_or_si256(
                    _mm256_or_si256(
                        _mm256_or_si256(
                            _mm256_or_si256(
                                _mm256_or_si256(
                                    _mm256_or_si256(
                                        _mm256_or_si256(
                                            _mm256_or_si256(
                                                _mm256_or_si256(
                                                    _mm256_or_si256(
                                                        _mm256_or_si256(
                                                            _mm256_and_si256(
                                                                sz_haswell_in_byte_range_(v, 131, 3),
                                                                _mm256_cmpeq_epi8(
                                                                    _mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                    _mm256_set1_epi8(1))),
                                                            _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)136))),
                                                        _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)140))),
                                                    _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)146))),
                                                _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)153))),
                                            _mm256_and_si256(sz_haswell_in_byte_range_(v, 161, 5),
                                                             _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                               _mm256_set1_epi8(1)))),
                                        _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)168))),
                                    _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)173))),
                                _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)176))),
                            _mm256_and_si256(
                                sz_haswell_in_byte_range_(v, 180, 3),
                                _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)), _mm256_set1_epi8(0)))),
                        _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)185))),
                    _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)189))))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match5, _mm256_set1_epi8((char)255)));
    __m256i match6 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pc4, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)191))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match6, _mm256_set1_epi8((char)193)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match6), _mm256_set1_epi8((char)1)));
    __m256i match7 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pc5, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)128))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match7, _mm256_set1_epi8((char)63)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match7), _mm256_set1_epi8((char)255)));
    __m256i match8 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pc5, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)184))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match8, _mm256_set1_epi8((char)7)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match8), _mm256_set1_epi8((char)254)));
    __m256i match9 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)128))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match9, _mm256_set1_epi8((char)3)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match9), _mm256_set1_epi8((char)3)));
    __m256i match10 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)129))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match10, _mm256_set1_epi8((char)18)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match10), _mm256_set1_epi8((char)3)));
    __m256i match11 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)134))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match11, _mm256_set1_epi8((char)14)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match11), _mm256_set1_epi8((char)3)));
    __m256i match12 = _mm256_andnot_si256(
        sz_haswell_previous_bytes_(capitalize, 1),
        _mm256_and_si256(pc6, _mm256_or_si256(sz_haswell_in_byte_range_(v, 137, 2),
                                              _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)147)))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match12, _mm256_set1_epi8((char)13)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match12), _mm256_set1_epi8((char)3)));
    __m256i match13 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)142))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match13, _mm256_set1_epi8((char)15)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match13), _mm256_set1_epi8((char)1)));
    __m256i match14 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)143))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match14, _mm256_set1_epi8((char)10)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match14), _mm256_set1_epi8((char)3)));
    __m256i match15 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)144))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match15, _mm256_set1_epi8((char)11)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match15), _mm256_set1_epi8((char)3)));
    __m256i match16 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)148))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match16, _mm256_set1_epi8((char)15)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match16), _mm256_set1_epi8((char)3)));
    __m256i match17 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)149))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match17, _mm256_set1_epi8((char)33)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match17), _mm256_set1_epi8((char)1)));
    __m256i match18 = _mm256_andnot_si256(
        sz_haswell_previous_bytes_(capitalize, 1),
        _mm256_and_si256(pc6, _mm256_or_si256(_mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)150)),
                                              _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)156)))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match18, _mm256_set1_epi8((char)19)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match18), _mm256_set1_epi8((char)3)));
    __m256i match19 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)151))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match19, _mm256_set1_epi8((char)17)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match19), _mm256_set1_epi8((char)3)));
    __m256i match20 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)154))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match20, _mm256_set1_epi8((char)35)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match20), _mm256_set1_epi8((char)2)));
    __m256i match21 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)157))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match21, _mm256_set1_epi8((char)21)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match21), _mm256_set1_epi8((char)3)));
    __m256i match22 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)158))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match22, _mm256_set1_epi8((char)2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match22), _mm256_set1_epi8((char)2)));
    __m256i match23 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)159))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match23, _mm256_set1_epi8((char)22)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match23), _mm256_set1_epi8((char)3)));
    __m256i match24 = _mm256_andnot_si256(
        sz_haswell_previous_bytes_(capitalize, 1),
        _mm256_and_si256(pc6, _mm256_or_si256(_mm256_or_si256(_mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)166)),
                                                              _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)169))),
                                              _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)174)))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match24, _mm256_set1_epi8((char)218)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match24), _mm256_set1_epi8((char)4)));
    __m256i match25 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pc6, sz_haswell_in_byte_range_(v, 177, 2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match25, _mm256_set1_epi8((char)217)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match25), _mm256_set1_epi8((char)4)));
    __m256i match26 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)183))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match26, _mm256_set1_epi8((char)219)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match26), _mm256_set1_epi8((char)4)));
    __m256i match27 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pc6, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)191))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match27, _mm256_set1_epi8((char)248)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match27), _mm256_set1_epi8((char)1)));
    __m256i match28 = _mm256_andnot_si256(
        sz_haswell_previous_bytes_(capitalize, 2),
        _mm256_or_si256(
            _mm256_or_si256(
                _mm256_or_si256(
                    _mm256_and_si256(pe1b8, _mm256_and_si256(sz_haswell_in_byte_range_(v, 128, 63),
                                                             _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                               _mm256_set1_epi8(0)))),
                    _mm256_and_si256(pe1b9, _mm256_and_si256(sz_haswell_in_byte_range_(v, 128, 63),
                                                             _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                               _mm256_set1_epi8(0))))),
                _mm256_and_si256(
                    pe1ba, _mm256_or_si256(_mm256_and_si256(sz_haswell_in_byte_range_(v, 128, 21),
                                                            _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                              _mm256_set1_epi8(0))),
                                           _mm256_and_si256(sz_haswell_in_byte_range_(v, 160, 31),
                                                            _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                              _mm256_set1_epi8(0)))))),
            _mm256_and_si256(pe1bb, _mm256_and_si256(sz_haswell_in_byte_range_(v, 128, 63),
                                                     _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                       _mm256_set1_epi8(0))))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match28, _mm256_set1_epi8((char)1)));
    __m256i match29 = _mm256_and_si256(
        sz_haswell_previous_bytes_(capitalize, 2),
        _mm256_or_si256(
            _mm256_or_si256(
                _mm256_or_si256(
                    _mm256_and_si256(pe1b8, _mm256_and_si256(sz_haswell_in_byte_range_(v, 129, 63),
                                                             _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                               _mm256_set1_epi8(1)))),
                    _mm256_and_si256(pe1b9, _mm256_and_si256(sz_haswell_in_byte_range_(v, 129, 63),
                                                             _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                               _mm256_set1_epi8(1))))),
                _mm256_and_si256(
                    pe1ba, _mm256_or_si256(_mm256_and_si256(sz_haswell_in_byte_range_(v, 129, 21),
                                                            _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                              _mm256_set1_epi8(1))),
                                           _mm256_and_si256(sz_haswell_in_byte_range_(v, 161, 31),
                                                            _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                              _mm256_set1_epi8(1)))))),
            _mm256_and_si256(pe1bb, _mm256_and_si256(sz_haswell_in_byte_range_(v, 129, 63),
                                                     _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                       _mm256_set1_epi8(1))))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match29, _mm256_set1_epi8((char)255)));
    __m256i match30 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                       _mm256_and_si256(pe1ba, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)155))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match30, _mm256_set1_epi8((char)5)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match30), _mm256_set1_epi8((char)255)));
    _mm256_storeu_si256((__m256i *)target, result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_haswell_cyrillic_(__m256i v, sz_ptr_t target, sz_bool_t *word_start) {
    __m256i prev1 = sz_haswell_previous_bytes_(v, 1);
    __m256i continuations = sz_haswell_in_byte_range_(v, 0x80, 0x40);
    sz_u32_t allowed = ~(sz_u32_t)_mm256_movemask_epi8(v);
    sz_u32_t alnum = (sz_u32_t)_mm256_movemask_epi8(
        _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26)),
                        sz_haswell_in_byte_range_(v, 48, 10)));
    sz_u32_t stop = 0;
    __m256i pd0 = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)208)), continuations);
    sz_u32_t pd0_bits = (sz_u32_t)_mm256_movemask_epi8(pd0);
    allowed |= (pd0_bits >> 0) | (pd0_bits >> 1);
    sz_u32_t pd0_word = (sz_u32_t)_mm256_movemask_epi8(pd0);
    alnum |= (pd0_word >> 0) | (pd0_word >> 1);
    __m256i pd1 = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)209)), continuations);
    sz_u32_t pd1_bits = (sz_u32_t)_mm256_movemask_epi8(pd1);
    allowed |= (pd1_bits >> 0) | (pd1_bits >> 1);
    sz_u32_t pd1_word = (sz_u32_t)_mm256_movemask_epi8(pd1);
    alnum |= (pd1_word >> 0) | (pd1_word >> 1);
    __m256i pd2 = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)210)), continuations);
    sz_u32_t pd2_bits = (sz_u32_t)_mm256_movemask_epi8(pd2);
    allowed |= (pd2_bits >> 0) | (pd2_bits >> 1);
    sz_u32_t pd2_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pd2, _mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 2), sz_haswell_in_byte_range_(v, 138, 54))));
    alnum |= (pd2_word >> 0) | (pd2_word >> 1);
    __m256i pd3 = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)211)), continuations);
    sz_u32_t pd3_bits = (sz_u32_t)_mm256_movemask_epi8(pd3);
    allowed |= (pd3_bits >> 0) | (pd3_bits >> 1);
    sz_u32_t pd3_word = (sz_u32_t)_mm256_movemask_epi8(pd3);
    alnum |= (pd3_word >> 0) | (pd3_word >> 1);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u32(stop) : 32;
    if (!length) return 0;
    sz_u32_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    __m256i capitalize = sz_utf8_initcap_haswell_expand_mask_(starts);
    __m256i letters = _mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26));
    __m256i result = _mm256_or_si256(v, _mm256_and_si256(letters, _mm256_set1_epi8(32)));
    result = _mm256_sub_epi8(result, _mm256_and_si256(_mm256_and_si256(capitalize, letters), _mm256_set1_epi8(32)));
    __m256i match0 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pd0, sz_haswell_in_byte_range_(v, 128, 16)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match0, _mm256_set1_epi8((char)16)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match0), _mm256_set1_epi8((char)1)));
    __m256i match1 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pd0, sz_haswell_in_byte_range_(v, 144, 16)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match1, _mm256_set1_epi8((char)32)));
    __m256i match2 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pd0, sz_haswell_in_byte_range_(v, 160, 16)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match2, _mm256_set1_epi8((char)224)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match2), _mm256_set1_epi8((char)1)));
    __m256i match3 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pd0, sz_haswell_in_byte_range_(v, 176, 16)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match3, _mm256_set1_epi8((char)224)));
    __m256i match4 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pd1, sz_haswell_in_byte_range_(v, 128, 16)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match4, _mm256_set1_epi8((char)32)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match4), _mm256_set1_epi8((char)255)));
    __m256i match5 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pd1, sz_haswell_in_byte_range_(v, 144, 16)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match5, _mm256_set1_epi8((char)240)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match5), _mm256_set1_epi8((char)255)));
    __m256i match6 = _mm256_andnot_si256(
        sz_haswell_previous_bytes_(capitalize, 1),
        _mm256_or_si256(
            _mm256_or_si256(
                _mm256_and_si256(pd1, _mm256_and_si256(sz_haswell_in_byte_range_(v, 160, 31),
                                                       _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                         _mm256_set1_epi8(0)))),
                _mm256_and_si256(
                    pd2, _mm256_or_si256(_mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)128)),
                                         _mm256_and_si256(sz_haswell_in_byte_range_(v, 138, 53),
                                                          _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                            _mm256_set1_epi8(0)))))),
            _mm256_and_si256(
                pd3, _mm256_or_si256(_mm256_and_si256(sz_haswell_in_byte_range_(v, 129, 13),
                                                      _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                        _mm256_set1_epi8(1))),
                                     _mm256_and_si256(sz_haswell_in_byte_range_(v, 144, 47),
                                                      _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                        _mm256_set1_epi8(0)))))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match6, _mm256_set1_epi8((char)1)));
    __m256i match7 = _mm256_and_si256(
        sz_haswell_previous_bytes_(capitalize, 1),
        _mm256_or_si256(
            _mm256_or_si256(
                _mm256_and_si256(pd1, _mm256_and_si256(sz_haswell_in_byte_range_(v, 161, 31),
                                                       _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                         _mm256_set1_epi8(1)))),
                _mm256_and_si256(
                    pd2, _mm256_or_si256(_mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)129)),
                                         _mm256_and_si256(sz_haswell_in_byte_range_(v, 139, 53),
                                                          _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                            _mm256_set1_epi8(1)))))),
            _mm256_and_si256(
                pd3, _mm256_or_si256(_mm256_and_si256(sz_haswell_in_byte_range_(v, 130, 13),
                                                      _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                        _mm256_set1_epi8(0))),
                                     _mm256_and_si256(sz_haswell_in_byte_range_(v, 145, 47),
                                                      _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                        _mm256_set1_epi8(1)))))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match7, _mm256_set1_epi8((char)255)));
    __m256i match8 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pd3, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)128))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match8, _mm256_set1_epi8((char)15)));
    __m256i match9 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pd3, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)143))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match9, _mm256_set1_epi8((char)241)));
    _mm256_storeu_si256((__m256i *)target, result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_haswell_greek_(__m256i v, sz_ptr_t target, sz_bool_t *word_start) {
    __m256i prev1 = sz_haswell_previous_bytes_(v, 1);
    __m256i prev2 = sz_haswell_previous_bytes_(v, 2);
    __m256i continuations = sz_haswell_in_byte_range_(v, 0x80, 0x40);
    sz_u32_t allowed = ~(sz_u32_t)_mm256_movemask_epi8(v);
    sz_u32_t alnum = (sz_u32_t)_mm256_movemask_epi8(
        _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26)),
                        sz_haswell_in_byte_range_(v, 48, 10)));
    sz_u32_t stop = 0;
    __m256i pce = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)206)), continuations);
    sz_u32_t pce_bits = (sz_u32_t)_mm256_movemask_epi8(pce);
    allowed |= (pce_bits >> 0) | (pce_bits >> 1);
    sz_u32_t pce_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pce,
        _mm256_or_si256(
            _mm256_or_si256(
                _mm256_or_si256(
                    _mm256_or_si256(_mm256_and_si256(sz_haswell_in_byte_range_(v, 134, 3),
                                                     _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                       _mm256_set1_epi8(0))),
                                    sz_haswell_in_byte_range_(v, 137, 2)),
                    _mm256_and_si256(sz_haswell_in_byte_range_(v, 140, 3),
                                     _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)), _mm256_set1_epi8(0)))),
                sz_haswell_in_byte_range_(v, 143, 19)),
            sz_haswell_in_byte_range_(v, 163, 29))));
    alnum |= (pce_word >> 0) | (pce_word >> 1);
    __m256i pcf = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)207)), continuations);
    sz_u32_t pcf_bits = (sz_u32_t)_mm256_movemask_epi8(pcf);
    allowed |= (pcf_bits >> 0) | (pcf_bits >> 1);
    sz_u32_t pcf_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pcf, _mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 54), sz_haswell_in_byte_range_(v, 183, 9))));
    alnum |= (pcf_word >> 0) | (pcf_word >> 1);
    __m256i pe1bc = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)188)),
                                                      _mm256_cmpeq_epi8(prev2, _mm256_set1_epi8((char)225))),
                                     continuations);
    sz_u32_t pe1bc_bits = (sz_u32_t)_mm256_movemask_epi8(pe1bc);
    allowed |= (pe1bc_bits >> 0) | (pe1bc_bits >> 1) | (pe1bc_bits >> 2);
    sz_u32_t pe1bc_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pe1bc,
        _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 22), sz_haswell_in_byte_range_(v, 152, 6)),
                        sz_haswell_in_byte_range_(v, 160, 32))));
    alnum |= (pe1bc_word >> 0) | (pe1bc_word >> 1) | (pe1bc_word >> 2);
    __m256i pe1bd = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)189)),
                                                      _mm256_cmpeq_epi8(prev2, _mm256_set1_epi8((char)225))),
                                     continuations);
    sz_u32_t pe1bd_bits = (sz_u32_t)_mm256_movemask_epi8(pe1bd);
    allowed |= (pe1bd_bits >> 0) | (pe1bd_bits >> 1) | (pe1bd_bits >> 2);
    sz_u32_t pe1bd_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pe1bd,
        _mm256_or_si256(_mm256_or_si256(_mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 6),
                                                                        sz_haswell_in_byte_range_(v, 136, 6)),
                                                        sz_haswell_in_byte_range_(v, 144, 8)),
                                        _mm256_and_si256(sz_haswell_in_byte_range_(v, 153, 7),
                                                         _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                           _mm256_set1_epi8(1)))),
                        sz_haswell_in_byte_range_(v, 160, 30))));
    alnum |= (pe1bd_word >> 0) | (pe1bd_word >> 1) | (pe1bd_word >> 2);
    __m256i pe1be = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)190)),
                                                      _mm256_cmpeq_epi8(prev2, _mm256_set1_epi8((char)225))),
                                     continuations);
    sz_u32_t pe1be_bits = (sz_u32_t)_mm256_movemask_epi8(pe1be);
    allowed |= (pe1be_bits >> 0) | (pe1be_bits >> 1) | (pe1be_bits >> 2);
    sz_u32_t pe1be_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pe1be,
        _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 53), sz_haswell_in_byte_range_(v, 182, 7)),
                        _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)190)))));
    alnum |= (pe1be_word >> 0) | (pe1be_word >> 1) | (pe1be_word >> 2);
    stop |=
        ((sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(pe1be, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)190))))) >>
        2;
    __m256i pe1bf = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)191)),
                                                      _mm256_cmpeq_epi8(prev2, _mm256_set1_epi8((char)225))),
                                     continuations);
    sz_u32_t pe1bf_bits = (sz_u32_t)_mm256_movemask_epi8(pe1bf);
    allowed |= (pe1bf_bits >> 0) | (pe1bf_bits >> 1) | (pe1bf_bits >> 2);
    sz_u32_t pe1bf_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pe1bf,
        _mm256_or_si256(
            _mm256_or_si256(
                _mm256_or_si256(_mm256_or_si256(_mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 130, 3),
                                                                                sz_haswell_in_byte_range_(v, 134, 7)),
                                                                sz_haswell_in_byte_range_(v, 144, 4)),
                                                sz_haswell_in_byte_range_(v, 150, 6)),
                                sz_haswell_in_byte_range_(v, 160, 13)),
                sz_haswell_in_byte_range_(v, 178, 3)),
            sz_haswell_in_byte_range_(v, 182, 7))));
    alnum |= (pe1bf_word >> 0) | (pe1bf_word >> 1) | (pe1bf_word >> 2);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u32(stop) : 32;
    if (!length) return 0;
    sz_u32_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    __m256i capitalize = sz_utf8_initcap_haswell_expand_mask_(starts);
    __m256i letters = _mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26));
    __m256i result = _mm256_or_si256(v, _mm256_and_si256(letters, _mm256_set1_epi8(32)));
    result = _mm256_sub_epi8(result, _mm256_and_si256(_mm256_and_si256(capitalize, letters), _mm256_set1_epi8(32)));
    __m256i match0 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pce, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)134))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match0, _mm256_set1_epi8((char)38)));
    __m256i match1 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pce, sz_haswell_in_byte_range_(v, 136, 3)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match1, _mm256_set1_epi8((char)37)));
    __m256i match2 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pce, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)140))));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match2), _mm256_set1_epi8((char)1)));
    __m256i match3 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pce, sz_haswell_in_byte_range_(v, 142, 2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match3, _mm256_set1_epi8((char)255)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match3), _mm256_set1_epi8((char)1)));
    __m256i match4 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pce, sz_haswell_in_byte_range_(v, 145, 15)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match4, _mm256_set1_epi8((char)32)));
    __m256i match5 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pce, _mm256_or_si256(sz_haswell_in_byte_range_(v, 160, 2),
                                                                               sz_haswell_in_byte_range_(v, 163, 9))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match5, _mm256_set1_epi8((char)224)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match5), _mm256_set1_epi8((char)1)));
    __m256i match6 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pce, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)172))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match6, _mm256_set1_epi8((char)218)));
    __m256i match7 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pce, sz_haswell_in_byte_range_(v, 173, 3)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match7, _mm256_set1_epi8((char)219)));
    __m256i match8 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pce, sz_haswell_in_byte_range_(v, 177, 15)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match8, _mm256_set1_epi8((char)224)));
    __m256i match9 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pcf, _mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 2),
                                                                            sz_haswell_in_byte_range_(v, 131, 9))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match9, _mm256_set1_epi8((char)32)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match9), _mm256_set1_epi8((char)255)));
    __m256i match10 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)130))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match10, _mm256_set1_epi8((char)33)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match10), _mm256_set1_epi8((char)255)));
    __m256i match11 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)140))));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match11), _mm256_set1_epi8((char)255)));
    __m256i match12 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, sz_haswell_in_byte_range_(v, 141, 2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match12, _mm256_set1_epi8((char)1)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match12), _mm256_set1_epi8((char)255)));
    __m256i match13 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)143))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match13, _mm256_set1_epi8((char)8)));
    __m256i match14 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)144))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match14, _mm256_set1_epi8((char)2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match14), _mm256_set1_epi8((char)255)));
    __m256i match15 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)145))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match15, _mm256_set1_epi8((char)7)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match15), _mm256_set1_epi8((char)255)));
    __m256i match16 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)149))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match16, _mm256_set1_epi8((char)17)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match16), _mm256_set1_epi8((char)255)));
    __m256i match17 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)150))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match17, _mm256_set1_epi8((char)10)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match17), _mm256_set1_epi8((char)255)));
    __m256i match18 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)151))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match18, _mm256_set1_epi8((char)248)));
    __m256i match19 = _mm256_andnot_si256(
        sz_haswell_previous_bytes_(capitalize, 1),
        _mm256_and_si256(
            pcf,
            _mm256_or_si256(_mm256_or_si256(_mm256_and_si256(sz_haswell_in_byte_range_(v, 152, 23),
                                                             _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                               _mm256_set1_epi8(0))),
                                            _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)183))),
                            _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)186)))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match19, _mm256_set1_epi8((char)1)));
    __m256i match20 = _mm256_and_si256(
        sz_haswell_previous_bytes_(capitalize, 1),
        _mm256_and_si256(
            pcf,
            _mm256_or_si256(_mm256_or_si256(_mm256_and_si256(sz_haswell_in_byte_range_(v, 153, 23),
                                                             _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                               _mm256_set1_epi8(1))),
                                            _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)184))),
                            _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)187)))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match20, _mm256_set1_epi8((char)255)));
    __m256i match21 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)176))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match21, _mm256_set1_epi8((char)234)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match21), _mm256_set1_epi8((char)255)));
    __m256i match22 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)177))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match22, _mm256_set1_epi8((char)240)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match22), _mm256_set1_epi8((char)255)));
    __m256i match23 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)178))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match23, _mm256_set1_epi8((char)7)));
    __m256i match24 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)179))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match24, _mm256_set1_epi8((char)12)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match24), _mm256_set1_epi8((char)254)));
    __m256i match25 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)180))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match25, _mm256_set1_epi8((char)4)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match25), _mm256_set1_epi8((char)255)));
    __m256i match26 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                       _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)181))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match26, _mm256_set1_epi8((char)224)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match26), _mm256_set1_epi8((char)255)));
    __m256i match27 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pcf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)185))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match27, _mm256_set1_epi8((char)249)));
    __m256i match28 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                          _mm256_and_si256(pcf, sz_haswell_in_byte_range_(v, 189, 3)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match28, _mm256_set1_epi8((char)254)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match28), _mm256_set1_epi8((char)254)));
    __m256i match29 = _mm256_and_si256(
        sz_haswell_previous_bytes_(capitalize, 2),
        _mm256_or_si256(
            _mm256_or_si256(
                _mm256_or_si256(
                    _mm256_and_si256(
                        pe1bc, _mm256_or_si256(_mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 8),
                                                                               sz_haswell_in_byte_range_(v, 144, 6)),
                                                               sz_haswell_in_byte_range_(v, 160, 8)),
                                               sz_haswell_in_byte_range_(v, 176, 8))),
                    _mm256_and_si256(
                        pe1bd,
                        _mm256_or_si256(
                            _mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 6),
                                            _mm256_and_si256(sz_haswell_in_byte_range_(v, 145, 7),
                                                             _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                               _mm256_set1_epi8(1)))),
                            sz_haswell_in_byte_range_(v, 160, 8)))),
                _mm256_and_si256(pe1be,
                                 _mm256_or_si256(_mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 8),
                                                                                 sz_haswell_in_byte_range_(v, 144, 8)),
                                                                 sz_haswell_in_byte_range_(v, 160, 8)),
                                                 sz_haswell_in_byte_range_(v, 176, 2)))),
            _mm256_and_si256(
                pe1bf, _mm256_or_si256(sz_haswell_in_byte_range_(v, 144, 2), sz_haswell_in_byte_range_(v, 160, 2)))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match29, _mm256_set1_epi8((char)8)));
    __m256i match30 = _mm256_andnot_si256(
        sz_haswell_previous_bytes_(capitalize, 2),
        _mm256_or_si256(
            _mm256_or_si256(
                _mm256_or_si256(
                    _mm256_and_si256(
                        pe1bc, _mm256_or_si256(_mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 136, 8),
                                                                               sz_haswell_in_byte_range_(v, 152, 6)),
                                                               sz_haswell_in_byte_range_(v, 168, 8)),
                                               sz_haswell_in_byte_range_(v, 184, 8))),
                    _mm256_and_si256(
                        pe1bd,
                        _mm256_or_si256(
                            _mm256_or_si256(sz_haswell_in_byte_range_(v, 136, 6),
                                            _mm256_and_si256(sz_haswell_in_byte_range_(v, 153, 7),
                                                             _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                               _mm256_set1_epi8(1)))),
                            sz_haswell_in_byte_range_(v, 168, 8)))),
                _mm256_and_si256(pe1be,
                                 _mm256_or_si256(_mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 136, 8),
                                                                                 sz_haswell_in_byte_range_(v, 152, 8)),
                                                                 sz_haswell_in_byte_range_(v, 168, 8)),
                                                 sz_haswell_in_byte_range_(v, 184, 2)))),
            _mm256_and_si256(
                pe1bf, _mm256_or_si256(sz_haswell_in_byte_range_(v, 152, 2), sz_haswell_in_byte_range_(v, 168, 2)))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match30, _mm256_set1_epi8((char)248)));
    __m256i match31 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                       _mm256_and_si256(pe1bd, sz_haswell_in_byte_range_(v, 176, 2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match31, _mm256_set1_epi8((char)10)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match31), _mm256_set1_epi8((char)1)));
    __m256i match32 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                       _mm256_and_si256(pe1bd, sz_haswell_in_byte_range_(v, 178, 4)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match32, _mm256_set1_epi8((char)214)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match32), _mm256_set1_epi8((char)2)));
    __m256i match33 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                       _mm256_and_si256(pe1bd, sz_haswell_in_byte_range_(v, 182, 2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match33, _mm256_set1_epi8((char)228)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match33), _mm256_set1_epi8((char)2)));
    __m256i match34 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                       _mm256_and_si256(pe1bd, sz_haswell_in_byte_range_(v, 184, 2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match34), _mm256_set1_epi8((char)2)));
    __m256i match35 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                       _mm256_and_si256(pe1bd, sz_haswell_in_byte_range_(v, 186, 2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match35, _mm256_set1_epi8((char)240)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match35), _mm256_set1_epi8((char)2)));
    __m256i match36 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                       _mm256_and_si256(pe1bd, sz_haswell_in_byte_range_(v, 188, 2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match36, _mm256_set1_epi8((char)254)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match36), _mm256_set1_epi8((char)2)));
    __m256i match37 = _mm256_and_si256(
        sz_haswell_previous_bytes_(capitalize, 2),
        _mm256_or_si256(_mm256_and_si256(pe1be, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)179))),
                        _mm256_and_si256(pe1bf, _mm256_or_si256(_mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)131)),
                                                                _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)179))))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match37, _mm256_set1_epi8((char)9)));
    __m256i match38 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                          _mm256_and_si256(pe1be, sz_haswell_in_byte_range_(v, 186, 2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match38, _mm256_set1_epi8((char)246)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match38), _mm256_set1_epi8((char)255)));
    __m256i match39 = _mm256_andnot_si256(
        sz_haswell_previous_bytes_(capitalize, 2),
        _mm256_or_si256(_mm256_and_si256(pe1be, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)188))),
                        _mm256_and_si256(pe1bf, _mm256_or_si256(_mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)140)),
                                                                _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)188))))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match39, _mm256_set1_epi8((char)247)));
    __m256i match40 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                          _mm256_and_si256(pe1bf, sz_haswell_in_byte_range_(v, 136, 4)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match40, _mm256_set1_epi8((char)42)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match40), _mm256_set1_epi8((char)254)));
    __m256i match41 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                          _mm256_and_si256(pe1bf, sz_haswell_in_byte_range_(v, 154, 2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match41, _mm256_set1_epi8((char)28)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match41), _mm256_set1_epi8((char)254)));
    __m256i match42 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                       _mm256_and_si256(pe1bf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)165))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match42, _mm256_set1_epi8((char)7)));
    __m256i match43 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                          _mm256_and_si256(pe1bf, sz_haswell_in_byte_range_(v, 170, 2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match43, _mm256_set1_epi8((char)16)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match43), _mm256_set1_epi8((char)254)));
    __m256i match44 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                          _mm256_and_si256(pe1bf, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)172))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match44, _mm256_set1_epi8((char)249)));
    __m256i match45 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                          _mm256_and_si256(pe1bf, sz_haswell_in_byte_range_(v, 184, 2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match45), _mm256_set1_epi8((char)254)));
    __m256i match46 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                          _mm256_and_si256(pe1bf, sz_haswell_in_byte_range_(v, 186, 2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match46, _mm256_set1_epi8((char)2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match46), _mm256_set1_epi8((char)254)));
    _mm256_storeu_si256((__m256i *)target, result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_haswell_georgian_(__m256i v, sz_ptr_t target, sz_bool_t *word_start) {
    __m256i prev1 = sz_haswell_previous_bytes_(v, 1);
    __m256i prev2 = sz_haswell_previous_bytes_(v, 2);
    __m256i continuations = sz_haswell_in_byte_range_(v, 0x80, 0x40);
    sz_u32_t allowed = ~(sz_u32_t)_mm256_movemask_epi8(v);
    sz_u32_t alnum = (sz_u32_t)_mm256_movemask_epi8(
        _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26)),
                        sz_haswell_in_byte_range_(v, 48, 10)));
    sz_u32_t stop = 0;
    __m256i pe182 = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)130)),
                                                      _mm256_cmpeq_epi8(prev2, _mm256_set1_epi8((char)225))),
                                     continuations);
    sz_u32_t pe182_bits = (sz_u32_t)_mm256_movemask_epi8(pe182);
    allowed |= (pe182_bits >> 0) | (pe182_bits >> 1) | (pe182_bits >> 2);
    sz_u32_t pe182_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pe182,
        _mm256_or_si256(
            _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 2),
                                            _mm256_and_si256(sz_haswell_in_byte_range_(v, 142, 3),
                                                             _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)),
                                                                               _mm256_set1_epi8(0)))),
                            sz_haswell_in_byte_range_(v, 145, 9)),
            sz_haswell_in_byte_range_(v, 160, 32))));
    alnum |= (pe182_word >> 0) | (pe182_word >> 1) | (pe182_word >> 2);
    __m256i pe183 = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)131)),
                                                      _mm256_cmpeq_epi8(prev2, _mm256_set1_epi8((char)225))),
                                     continuations);
    sz_u32_t pe183_bits = (sz_u32_t)_mm256_movemask_epi8(pe183);
    allowed |= (pe183_bits >> 0) | (pe183_bits >> 1) | (pe183_bits >> 2);
    sz_u32_t pe183_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pe183, _mm256_or_si256(
                   _mm256_or_si256(_mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 6),
                                                                   _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)135))),
                                                   _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)141))),
                                   sz_haswell_in_byte_range_(v, 144, 43)),
                   sz_haswell_in_byte_range_(v, 188, 4))));
    alnum |= (pe183_word >> 0) | (pe183_word >> 1) | (pe183_word >> 2);
    __m256i pe1b2 = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)178)),
                                                      _mm256_cmpeq_epi8(prev2, _mm256_set1_epi8((char)225))),
                                     continuations);
    sz_u32_t pe1b2_bits = (sz_u32_t)_mm256_movemask_epi8(pe1b2);
    allowed |= (pe1b2_bits >> 0) | (pe1b2_bits >> 1) | (pe1b2_bits >> 2);
    sz_u32_t pe1b2_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pe1b2,
        _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 11), sz_haswell_in_byte_range_(v, 144, 43)),
                        sz_haswell_in_byte_range_(v, 189, 3))));
    alnum |= (pe1b2_word >> 0) | (pe1b2_word >> 1) | (pe1b2_word >> 2);
    stop |= ((sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(pe1b2, sz_haswell_in_byte_range_(v, 128, 8)))) >> 2;
    __m256i pe1b3 = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)179)),
                                                      _mm256_cmpeq_epi8(prev2, _mm256_set1_epi8((char)225))),
                                     continuations);
    sz_u32_t pe1b3_bits = (sz_u32_t)_mm256_movemask_epi8(pe1b3);
    allowed |= (pe1b3_bits >> 0) | (pe1b3_bits >> 1) | (pe1b3_bits >> 2);
    sz_u32_t pe1b3_word = (sz_u32_t)_mm256_movemask_epi8(
        _mm256_and_si256(pe1b3, _mm256_or_si256(_mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 169, 4),
                                                                                sz_haswell_in_byte_range_(v, 174, 6)),
                                                                sz_haswell_in_byte_range_(v, 181, 2)),
                                                _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)186)))));
    alnum |= (pe1b3_word >> 0) | (pe1b3_word >> 1) | (pe1b3_word >> 2);
    __m256i pe2b4 = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)180)),
                                                      _mm256_cmpeq_epi8(prev2, _mm256_set1_epi8((char)226))),
                                     continuations);
    sz_u32_t pe2b4_bits = (sz_u32_t)_mm256_movemask_epi8(pe2b4);
    allowed |= (pe2b4_bits >> 0) | (pe2b4_bits >> 1) | (pe2b4_bits >> 2);
    sz_u32_t pe2b4_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pe2b4, _mm256_or_si256(_mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 38),
                                                               _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)167))),
                                               _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)173))),
                               sz_haswell_in_byte_range_(v, 176, 16))));
    alnum |= (pe2b4_word >> 0) | (pe2b4_word >> 1) | (pe2b4_word >> 2);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u32(stop) : 32;
    if (!length) return 0;
    sz_u32_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    __m256i capitalize = sz_utf8_initcap_haswell_expand_mask_(starts);
    __m256i letters = _mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26));
    __m256i result = _mm256_or_si256(v, _mm256_and_si256(letters, _mm256_set1_epi8(32)));
    result = _mm256_sub_epi8(result, _mm256_and_si256(_mm256_and_si256(capitalize, letters), _mm256_set1_epi8(32)));
    __m256i match0 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                         _mm256_and_si256(pe182, sz_haswell_in_byte_range_(v, 160, 32)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match0, _mm256_set1_epi8((char)224)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match0), _mm256_set1_epi8((char)50)));
    result = _mm256_add_epi8(
        result, _mm256_and_si256(sz_haswell_next_bytes_(sz_haswell_next_bytes_(match0)), _mm256_set1_epi8((char)1)));
    __m256i match1 = _mm256_andnot_si256(
        sz_haswell_previous_bytes_(capitalize, 2),
        _mm256_and_si256(pe183, _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 6),
                                                                _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)135))),
                                                _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)141)))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match1, _mm256_set1_epi8((char)32)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match1), _mm256_set1_epi8((char)49)));
    result = _mm256_add_epi8(
        result, _mm256_and_si256(sz_haswell_next_bytes_(sz_haswell_next_bytes_(match1)), _mm256_set1_epi8((char)1)));
    __m256i match2 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                      _mm256_and_si256(pe183, _mm256_or_si256(sz_haswell_in_byte_range_(v, 144, 43),
                                                                              sz_haswell_in_byte_range_(v, 189, 3))));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match2), _mm256_set1_epi8((char)47)));
    __m256i match3 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                      _mm256_and_si256(pe1b2, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)136))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match3, _mm256_set1_epi8((char)2)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match3), _mm256_set1_epi8((char)231)));
    result = _mm256_add_epi8(
        result, _mm256_and_si256(sz_haswell_next_bytes_(sz_haswell_next_bytes_(match3)), _mm256_set1_epi8((char)9)));
    __m256i match4 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                         _mm256_and_si256(pe1b2, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)137))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match4, _mm256_set1_epi8((char)1)));
    __m256i match5 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                      _mm256_and_si256(pe1b2, _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)138))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match5, _mm256_set1_epi8((char)255)));
    __m256i match6 = _mm256_andnot_si256(
        sz_haswell_previous_bytes_(capitalize, 2),
        _mm256_and_si256(pe1b2,
                         _mm256_or_si256(sz_haswell_in_byte_range_(v, 144, 43), sz_haswell_in_byte_range_(v, 189, 3))));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match6), _mm256_set1_epi8((char)209)));
    __m256i match7 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                      _mm256_and_si256(pe2b4, sz_haswell_in_byte_range_(v, 128, 32)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match7, _mm256_set1_epi8((char)32)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match7), _mm256_set1_epi8((char)206)));
    result = _mm256_add_epi8(
        result, _mm256_and_si256(sz_haswell_next_bytes_(sz_haswell_next_bytes_(match7)), _mm256_set1_epi8((char)255)));
    __m256i match8 = _mm256_and_si256(
        sz_haswell_previous_bytes_(capitalize, 2),
        _mm256_and_si256(pe2b4, _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 160, 6),
                                                                _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)167))),
                                                _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)173)))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match8, _mm256_set1_epi8((char)224)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match8), _mm256_set1_epi8((char)207)));
    result = _mm256_add_epi8(
        result, _mm256_and_si256(sz_haswell_next_bytes_(sz_haswell_next_bytes_(match8)), _mm256_set1_epi8((char)255)));
    _mm256_storeu_si256((__m256i *)target, result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_haswell_armenian_(__m256i v, sz_ptr_t target, sz_bool_t *word_start) {
    __m256i prev1 = sz_haswell_previous_bytes_(v, 1);
    __m256i continuations = sz_haswell_in_byte_range_(v, 0x80, 0x40);
    sz_u32_t allowed = ~(sz_u32_t)_mm256_movemask_epi8(v);
    sz_u32_t alnum = (sz_u32_t)_mm256_movemask_epi8(
        _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26)),
                        sz_haswell_in_byte_range_(v, 48, 10)));
    sz_u32_t stop = 0;
    __m256i pd4 = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)212)), continuations);
    sz_u32_t pd4_bits = (sz_u32_t)_mm256_movemask_epi8(pd4);
    allowed |= (pd4_bits >> 0) | (pd4_bits >> 1);
    sz_u32_t pd4_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pd4, _mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 48), sz_haswell_in_byte_range_(v, 177, 15))));
    alnum |= (pd4_word >> 0) | (pd4_word >> 1);
    __m256i pd5 = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)213)), continuations);
    sz_u32_t pd5_bits = (sz_u32_t)_mm256_movemask_epi8(pd5);
    allowed |= (pd5_bits >> 0) | (pd5_bits >> 1);
    sz_u32_t pd5_word = (sz_u32_t)_mm256_movemask_epi8(
        _mm256_and_si256(pd5, _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 128, 23),
                                                              _mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)153))),
                                              sz_haswell_in_byte_range_(v, 160, 32))));
    alnum |= (pd5_word >> 0) | (pd5_word >> 1);
    __m256i pd6 = _mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)214)), continuations);
    sz_u32_t pd6_bits = (sz_u32_t)_mm256_movemask_epi8(pd6);
    allowed |= (pd6_bits >> 0) | (pd6_bits >> 1);
    sz_u32_t pd6_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(pd6, sz_haswell_in_byte_range_(v, 128, 9)));
    alnum |= (pd6_word >> 0) | (pd6_word >> 1);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u32(stop) : 32;
    if (!length) return 0;
    sz_u32_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    __m256i capitalize = sz_utf8_initcap_haswell_expand_mask_(starts);
    __m256i letters = _mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26));
    __m256i result = _mm256_or_si256(v, _mm256_and_si256(letters, _mm256_set1_epi8(32)));
    result = _mm256_sub_epi8(result, _mm256_and_si256(_mm256_and_si256(capitalize, letters), _mm256_set1_epi8(32)));
    __m256i match0 = _mm256_andnot_si256(
        sz_haswell_previous_bytes_(capitalize, 1),
        _mm256_and_si256(
            pd4, _mm256_and_si256(sz_haswell_in_byte_range_(v, 128, 47),
                                  _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)), _mm256_set1_epi8(0)))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match0, _mm256_set1_epi8((char)1)));
    __m256i match1 = _mm256_and_si256(
        sz_haswell_previous_bytes_(capitalize, 1),
        _mm256_and_si256(
            pd4, _mm256_and_si256(sz_haswell_in_byte_range_(v, 129, 47),
                                  _mm256_cmpeq_epi8(_mm256_and_si256(v, _mm256_set1_epi8(1)), _mm256_set1_epi8(1)))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match1, _mm256_set1_epi8((char)255)));
    __m256i match2 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_or_si256(_mm256_and_si256(pd4, sz_haswell_in_byte_range_(v, 177, 15)),
                                                         _mm256_and_si256(pd5, sz_haswell_in_byte_range_(v, 144, 7))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match2, _mm256_set1_epi8((char)240)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match2), _mm256_set1_epi8((char)1)));
    __m256i match3 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                         _mm256_and_si256(pd5, sz_haswell_in_byte_range_(v, 128, 16)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match3, _mm256_set1_epi8((char)48)));
    __m256i match4 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_or_si256(_mm256_and_si256(pd5, sz_haswell_in_byte_range_(v, 161, 15)),
                                                      _mm256_and_si256(pd6, sz_haswell_in_byte_range_(v, 128, 7))));
    result = _mm256_add_epi8(result, _mm256_and_si256(match4, _mm256_set1_epi8((char)16)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match4), _mm256_set1_epi8((char)255)));
    __m256i match5 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 1),
                                      _mm256_and_si256(pd5, sz_haswell_in_byte_range_(v, 176, 16)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match5, _mm256_set1_epi8((char)208)));
    _mm256_storeu_si256((__m256i *)target, result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_haswell_fullwidth_(__m256i v, sz_ptr_t target, sz_bool_t *word_start) {
    __m256i prev1 = sz_haswell_previous_bytes_(v, 1);
    __m256i prev2 = sz_haswell_previous_bytes_(v, 2);
    __m256i continuations = sz_haswell_in_byte_range_(v, 0x80, 0x40);
    sz_u32_t allowed = ~(sz_u32_t)_mm256_movemask_epi8(v);
    sz_u32_t alnum = (sz_u32_t)_mm256_movemask_epi8(
        _mm256_or_si256(_mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26)),
                        sz_haswell_in_byte_range_(v, 48, 10)));
    sz_u32_t stop = 0;
    __m256i pefbc = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)188)),
                                                      _mm256_cmpeq_epi8(prev2, _mm256_set1_epi8((char)239))),
                                     continuations);
    sz_u32_t pefbc_bits = (sz_u32_t)_mm256_movemask_epi8(pefbc);
    allowed |= (pefbc_bits >> 0) | (pefbc_bits >> 1) | (pefbc_bits >> 2);
    sz_u32_t pefbc_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pefbc, _mm256_or_si256(sz_haswell_in_byte_range_(v, 144, 10), sz_haswell_in_byte_range_(v, 161, 26))));
    alnum |= (pefbc_word >> 0) | (pefbc_word >> 1) | (pefbc_word >> 2);
    __m256i pefbd = _mm256_and_si256(_mm256_and_si256(_mm256_cmpeq_epi8(prev1, _mm256_set1_epi8((char)189)),
                                                      _mm256_cmpeq_epi8(prev2, _mm256_set1_epi8((char)239))),
                                     continuations);
    sz_u32_t pefbd_bits = (sz_u32_t)_mm256_movemask_epi8(pefbd);
    allowed |= (pefbd_bits >> 0) | (pefbd_bits >> 1) | (pefbd_bits >> 2);
    sz_u32_t pefbd_word = (sz_u32_t)_mm256_movemask_epi8(_mm256_and_si256(
        pefbd, _mm256_or_si256(sz_haswell_in_byte_range_(v, 129, 26), sz_haswell_in_byte_range_(v, 166, 26))));
    alnum |= (pefbd_word >> 0) | (pefbd_word >> 1) | (pefbd_word >> 2);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u32(stop) : 32;
    if (!length) return 0;
    sz_u32_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    __m256i capitalize = sz_utf8_initcap_haswell_expand_mask_(starts);
    __m256i letters = _mm256_or_si256(sz_haswell_in_byte_range_(v, 65, 26), sz_haswell_in_byte_range_(v, 97, 26));
    __m256i result = _mm256_or_si256(v, _mm256_and_si256(letters, _mm256_set1_epi8(32)));
    result = _mm256_sub_epi8(result, _mm256_and_si256(_mm256_and_si256(capitalize, letters), _mm256_set1_epi8(32)));
    __m256i match0 = _mm256_andnot_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                         _mm256_and_si256(pefbc, sz_haswell_in_byte_range_(v, 161, 26)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match0, _mm256_set1_epi8((char)224)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match0), _mm256_set1_epi8((char)1)));
    __m256i match1 = _mm256_and_si256(sz_haswell_previous_bytes_(capitalize, 2),
                                      _mm256_and_si256(pefbd, sz_haswell_in_byte_range_(v, 129, 26)));
    result = _mm256_add_epi8(result, _mm256_and_si256(match1, _mm256_set1_epi8((char)32)));
    result = _mm256_add_epi8(result, _mm256_and_si256(sz_haswell_next_bytes_(match1), _mm256_set1_epi8((char)255)));
    _mm256_storeu_si256((__m256i *)target, result);
    return length;
}
#endif

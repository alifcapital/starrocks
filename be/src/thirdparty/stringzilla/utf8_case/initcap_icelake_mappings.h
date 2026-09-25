/* Generated Unicode 17 simple INITCAP transforms. See UNICODE-LICENSE.txt. */
#ifndef STRINGZILLA_UTF8_INITCAP_ICELAKE_MAPPINGS_H_
#define STRINGZILLA_UTF8_INITCAP_ICELAKE_MAPPINGS_H_
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_icelake_greek_basic_(__m512i v, sz_ptr_t target, sz_bool_t *word_start) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t alnum = ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26))) |
                      _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)48)), _mm512_set1_epi8(10)));
    sz_u64_t stop = 0;
    sz_u64_t pce = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)206)) << 1) & continuations);
    sz_u64_t pce_bits = pce;
    allowed |= (pce_bits >> 0) | (pce_bits >> 1);
    sz_u64_t pce_word =
        (pce & (((((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)134)), _mm512_set1_epi8(3)) &
                    _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))) |
                   _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)137)), _mm512_set1_epi8(2))) |
                  (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)140)), _mm512_set1_epi8(3)) &
                   _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))) |
                 _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)143)), _mm512_set1_epi8(19))) |
                _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)163)), _mm512_set1_epi8(29))));
    alnum |= (pce_word >> 0) | (pce_word >> 1);
    sz_u64_t pcf = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)207)) << 1) & continuations);
    sz_u64_t pcf_bits = pcf;
    allowed |= (pcf_bits >> 0) | (pcf_bits >> 1);
    sz_u64_t pcf_word =
        (pcf & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(54)) |
                _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)183)), _mm512_set1_epi8(9))));
    alnum |= (pcf_word >> 0) | (pcf_word >> 1);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    sz_u64_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    sz_u64_t letters = (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26)));
    __m512i result = _mm512_mask_mov_epi8(v, letters, _mm512_or_si512(v, _mm512_set1_epi8(32)));
    result = _mm512_mask_sub_epi8(result, starts & letters, result, _mm512_set1_epi8(32));
    sz_u64_t match0 = (~(starts << 1) & ((pce & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)134)))));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)38));
    sz_u64_t match1 = (~(starts << 1) & ((pce & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)136)),
                                                                       _mm512_set1_epi8(3)))));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)37));
    sz_u64_t match2 = (~(starts << 1) & ((pce & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)140)))));
    result = _mm512_mask_add_epi8(result, match2 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match3 = (~(starts << 1) & ((pce & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)142)),
                                                                       _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)255));
    result = _mm512_mask_add_epi8(result, match3 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match4 = (~(starts << 1) & ((pce & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)145)),
                                                                       _mm512_set1_epi8(15)))));
    result = _mm512_mask_add_epi8(result, match4 >> 0, result, _mm512_set1_epi8((char)32));
    sz_u64_t match5 =
        (~(starts << 1) &
         ((pce & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(2)) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)163)), _mm512_set1_epi8(9))))));
    result = _mm512_mask_add_epi8(result, match5 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match5 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match6 = ((starts << 1) & ((pce & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)172)))));
    result = _mm512_mask_add_epi8(result, match6 >> 0, result, _mm512_set1_epi8((char)218));
    sz_u64_t match7 = ((starts << 1) & ((pce & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)173)),
                                                                      _mm512_set1_epi8(3)))));
    result = _mm512_mask_add_epi8(result, match7 >> 0, result, _mm512_set1_epi8((char)219));
    sz_u64_t match8 = ((starts << 1) & ((pce & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)177)),
                                                                      _mm512_set1_epi8(15)))));
    result = _mm512_mask_add_epi8(result, match8 >> 0, result, _mm512_set1_epi8((char)224));
    sz_u64_t match9 =
        ((starts << 1) &
         ((pcf & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(2)) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)131)), _mm512_set1_epi8(9))))));
    result = _mm512_mask_add_epi8(result, match9 >> 0, result, _mm512_set1_epi8((char)32));
    result = _mm512_mask_add_epi8(result, match9 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match10 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)130)))));
    result = _mm512_mask_add_epi8(result, match10 >> 0, result, _mm512_set1_epi8((char)33));
    result = _mm512_mask_add_epi8(result, match10 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match11 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)140)))));
    result = _mm512_mask_add_epi8(result, match11 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match12 = ((starts << 1) & ((pcf & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)141)),
                                                                       _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match12 >> 0, result, _mm512_set1_epi8((char)1));
    result = _mm512_mask_add_epi8(result, match12 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match13 = (~(starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)143)))));
    result = _mm512_mask_add_epi8(result, match13 >> 0, result, _mm512_set1_epi8((char)8));
    sz_u64_t match14 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)144)))));
    result = _mm512_mask_add_epi8(result, match14 >> 0, result, _mm512_set1_epi8((char)2));
    result = _mm512_mask_add_epi8(result, match14 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match15 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)145)))));
    result = _mm512_mask_add_epi8(result, match15 >> 0, result, _mm512_set1_epi8((char)7));
    result = _mm512_mask_add_epi8(result, match15 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match16 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)149)))));
    result = _mm512_mask_add_epi8(result, match16 >> 0, result, _mm512_set1_epi8((char)17));
    result = _mm512_mask_add_epi8(result, match16 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match17 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)150)))));
    result = _mm512_mask_add_epi8(result, match17 >> 0, result, _mm512_set1_epi8((char)10));
    result = _mm512_mask_add_epi8(result, match17 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match18 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)151)))));
    result = _mm512_mask_add_epi8(result, match18 >> 0, result, _mm512_set1_epi8((char)248));
    sz_u64_t match19 =
        (~(starts << 1) &
         ((pcf & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)152)), _mm512_set1_epi8(23)) &
                    _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))) |
                   _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)183))) |
                  _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)186))))));
    result = _mm512_mask_add_epi8(result, match19 >> 0, result, _mm512_set1_epi8((char)1));
    sz_u64_t match20 =
        ((starts << 1) &
         ((pcf & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)153)), _mm512_set1_epi8(23)) &
                    _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))) |
                   _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)184))) |
                  _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)187))))));
    result = _mm512_mask_add_epi8(result, match20 >> 0, result, _mm512_set1_epi8((char)255));
    sz_u64_t match21 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)176)))));
    result = _mm512_mask_add_epi8(result, match21 >> 0, result, _mm512_set1_epi8((char)234));
    result = _mm512_mask_add_epi8(result, match21 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match22 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)177)))));
    result = _mm512_mask_add_epi8(result, match22 >> 0, result, _mm512_set1_epi8((char)240));
    result = _mm512_mask_add_epi8(result, match22 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match23 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)178)))));
    result = _mm512_mask_add_epi8(result, match23 >> 0, result, _mm512_set1_epi8((char)7));
    sz_u64_t match24 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)179)))));
    result = _mm512_mask_add_epi8(result, match24 >> 0, result, _mm512_set1_epi8((char)12));
    result = _mm512_mask_add_epi8(result, match24 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match25 = (~(starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)180)))));
    result = _mm512_mask_add_epi8(result, match25 >> 0, result, _mm512_set1_epi8((char)4));
    result = _mm512_mask_add_epi8(result, match25 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match26 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)181)))));
    result = _mm512_mask_add_epi8(result, match26 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match26 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match27 = (~(starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)185)))));
    result = _mm512_mask_add_epi8(result, match27 >> 0, result, _mm512_set1_epi8((char)249));
    sz_u64_t match28 = (~(starts << 1) & ((pcf & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)189)),
                                                                        _mm512_set1_epi8(3)))));
    result = _mm512_mask_add_epi8(result, match28 >> 0, result, _mm512_set1_epi8((char)254));
    result = _mm512_mask_add_epi8(result, match28 >> 1, result, _mm512_set1_epi8((char)254));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_icelake_latin1_(__m512i v, sz_ptr_t target, sz_bool_t *word_start) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t alnum = ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26))) |
                      _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)48)), _mm512_set1_epi8(10)));
    sz_u64_t stop = 0;
    sz_u64_t pc2 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)194)) << 1) & continuations);
    sz_u64_t pc2_bits = pc2;
    allowed |= (pc2_bits >> 0) | (pc2_bits >> 1);
    sz_u64_t pc2_word = (pc2 & ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)170)) |
                                 _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)181))) |
                                _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)186))));
    alnum |= (pc2_word >> 0) | (pc2_word >> 1);
    sz_u64_t pc3 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)195)) << 1) & continuations);
    sz_u64_t pc3_bits = pc3;
    allowed |= (pc3_bits >> 0) | (pc3_bits >> 1);
    sz_u64_t pc3_word =
        (pc3 & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(23)) |
                 _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)152)), _mm512_set1_epi8(31))) |
                _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)184)), _mm512_set1_epi8(8))));
    alnum |= (pc3_word >> 0) | (pc3_word >> 1);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    sz_u64_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    sz_u64_t letters = (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26)));
    __m512i result = _mm512_mask_mov_epi8(v, letters, _mm512_or_si512(v, _mm512_set1_epi8(32)));
    result = _mm512_mask_sub_epi8(result, starts & letters, result, _mm512_set1_epi8(32));
    sz_u64_t match0 = ((starts << 1) & ((pc2 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)181)))));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)231));
    result = _mm512_mask_add_epi8(result, match0 >> 1, result, _mm512_set1_epi8((char)12));
    sz_u64_t match1 =
        (~(starts << 1) &
         ((pc3 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(23)) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)152)), _mm512_set1_epi8(7))))));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)32));
    sz_u64_t match2 =
        ((starts << 1) &
         ((pc3 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(23)) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)184)), _mm512_set1_epi8(7))))));
    result = _mm512_mask_add_epi8(result, match2 >> 0, result, _mm512_set1_epi8((char)224));
    sz_u64_t match3 = ((starts << 1) & ((pc3 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)191)))));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)249));
    result = _mm512_mask_add_epi8(result, match3 >> 1, result, _mm512_set1_epi8((char)2));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_icelake_latin_(__m512i v, sz_ptr_t target, sz_bool_t *word_start) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t alnum = ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26))) |
                      _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)48)), _mm512_set1_epi8(10)));
    sz_u64_t stop = 0;
    sz_u64_t pc2 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)194)) << 1) & continuations);
    sz_u64_t pc2_bits = pc2;
    allowed |= (pc2_bits >> 0) | (pc2_bits >> 1);
    sz_u64_t pc2_word = (pc2 & ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)170)) |
                                 _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)181))) |
                                _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)186))));
    alnum |= (pc2_word >> 0) | (pc2_word >> 1);
    sz_u64_t pc3 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)195)) << 1) & continuations);
    sz_u64_t pc3_bits = pc3;
    allowed |= (pc3_bits >> 0) | (pc3_bits >> 1);
    sz_u64_t pc3_word =
        (pc3 & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(23)) |
                 _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)152)), _mm512_set1_epi8(31))) |
                _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)184)), _mm512_set1_epi8(8))));
    alnum |= (pc3_word >> 0) | (pc3_word >> 1);
    sz_u64_t pc4 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)196)) << 1) & continuations);
    sz_u64_t pc4_bits = pc4;
    allowed |= (pc4_bits >> 0) | (pc4_bits >> 1);
    sz_u64_t pc4_word = pc4;
    alnum |= (pc4_word >> 0) | (pc4_word >> 1);
    stop |= ((pc4 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)176)), _mm512_set1_epi8(2)))) >> 1;
    sz_u64_t pc5 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)197)) << 1) & continuations);
    sz_u64_t pc5_bits = pc5;
    allowed |= (pc5_bits >> 0) | (pc5_bits >> 1);
    sz_u64_t pc5_word = pc5;
    alnum |= (pc5_word >> 0) | (pc5_word >> 1);
    stop |= ((pc5 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)191)))) >> 1;
    sz_u64_t pc6 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)198)) << 1) & continuations);
    sz_u64_t pc6_bits = pc6;
    allowed |= (pc6_bits >> 0) | (pc6_bits >> 1);
    sz_u64_t pc6_word = pc6;
    alnum |= (pc6_word >> 0) | (pc6_word >> 1);
    stop |= ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)155)))) >> 1;
    sz_u64_t pe1b8 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)184)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1b8_bits = pe1b8;
    allowed |= (pe1b8_bits >> 0) | (pe1b8_bits >> 1) | (pe1b8_bits >> 2);
    sz_u64_t pe1b8_word = pe1b8;
    alnum |= (pe1b8_word >> 0) | (pe1b8_word >> 1) | (pe1b8_word >> 2);
    sz_u64_t pe1b9 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)185)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1b9_bits = pe1b9;
    allowed |= (pe1b9_bits >> 0) | (pe1b9_bits >> 1) | (pe1b9_bits >> 2);
    sz_u64_t pe1b9_word = pe1b9;
    alnum |= (pe1b9_word >> 0) | (pe1b9_word >> 1) | (pe1b9_word >> 2);
    sz_u64_t pe1ba = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)186)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1ba_bits = pe1ba;
    allowed |= (pe1ba_bits >> 0) | (pe1ba_bits >> 1) | (pe1ba_bits >> 2);
    sz_u64_t pe1ba_word = pe1ba;
    alnum |= (pe1ba_word >> 0) | (pe1ba_word >> 1) | (pe1ba_word >> 2);
    stop |= ((pe1ba & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)158)))) >> 2;
    sz_u64_t pe1bb = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)187)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1bb_bits = pe1bb;
    allowed |= (pe1bb_bits >> 0) | (pe1bb_bits >> 1) | (pe1bb_bits >> 2);
    sz_u64_t pe1bb_word = pe1bb;
    alnum |= (pe1bb_word >> 0) | (pe1bb_word >> 1) | (pe1bb_word >> 2);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    sz_u64_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    sz_u64_t letters = (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26)));
    __m512i result = _mm512_mask_mov_epi8(v, letters, _mm512_or_si512(v, _mm512_set1_epi8(32)));
    result = _mm512_mask_sub_epi8(result, starts & letters, result, _mm512_set1_epi8(32));
    sz_u64_t match0 = ((starts << 1) & ((pc2 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)181)))));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)231));
    result = _mm512_mask_add_epi8(result, match0 >> 1, result, _mm512_set1_epi8((char)12));
    sz_u64_t match1 =
        (~(starts << 1) &
         ((pc3 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(23)) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)152)), _mm512_set1_epi8(7))))));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)32));
    sz_u64_t match2 =
        ((starts << 1) &
         ((pc3 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(23)) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)184)), _mm512_set1_epi8(7))))));
    result = _mm512_mask_add_epi8(result, match2 >> 0, result, _mm512_set1_epi8((char)224));
    sz_u64_t match3 = ((starts << 1) & ((pc3 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)191)))));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)249));
    result = _mm512_mask_add_epi8(result, match3 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match4 =
        (~(starts << 1) &
         ((((pc4 & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(47)) &
                      _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))) |
                     (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)178)), _mm512_set1_epi8(5)) &
                      _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))) |
                    (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)185)), _mm512_set1_epi8(5)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))))) |
            (pc5 & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(7)) &
                      _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))) |
                     (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)138)), _mm512_set1_epi8(45)) &
                      _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))) |
                    (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)185)), _mm512_set1_epi8(5)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))))) |
           (pc6 &
            ((((((((((((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)130)), _mm512_set1_epi8(3)) &
                        _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))) |
                       _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)135))) |
                      _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)139))) |
                     _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)145))) |
                    _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)152))) |
                   (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(5)) &
                    _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))) |
                  _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)167))) |
                 _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)172))) |
                _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)175))) |
               (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)179)), _mm512_set1_epi8(3)) &
                _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))) |
              _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)184))) |
             _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)188)))))));
    result = _mm512_mask_add_epi8(result, match4 >> 0, result, _mm512_set1_epi8((char)1));
    sz_u64_t match5 =
        ((starts << 1) &
         ((((pc4 & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(47)) &
                      _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))) |
                     (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)179)), _mm512_set1_epi8(5)) &
                      _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))) |
                    (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)186)), _mm512_set1_epi8(5)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))))) |
            (pc5 & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)130)), _mm512_set1_epi8(7)) &
                      _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))) |
                     (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)139)), _mm512_set1_epi8(45)) &
                      _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))) |
                    (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)186)), _mm512_set1_epi8(5)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))))) |
           (pc6 &
            ((((((((((((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)131)), _mm512_set1_epi8(3)) &
                        _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))) |
                       _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)136))) |
                      _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)140))) |
                     _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)146))) |
                    _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)153))) |
                   (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)161)), _mm512_set1_epi8(5)) &
                    _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))) |
                  _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)168))) |
                 _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)173))) |
                _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)176))) |
               (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)180)), _mm512_set1_epi8(3)) &
                _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))) |
              _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)185))) |
             _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)189)))))));
    result = _mm512_mask_add_epi8(result, match5 >> 0, result, _mm512_set1_epi8((char)255));
    sz_u64_t match6 = (~(starts << 1) & ((pc4 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)191)))));
    result = _mm512_mask_add_epi8(result, match6 >> 0, result, _mm512_set1_epi8((char)193));
    result = _mm512_mask_add_epi8(result, match6 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match7 = ((starts << 1) & ((pc5 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)128)))));
    result = _mm512_mask_add_epi8(result, match7 >> 0, result, _mm512_set1_epi8((char)63));
    result = _mm512_mask_add_epi8(result, match7 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match8 = (~(starts << 1) & ((pc5 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)184)))));
    result = _mm512_mask_add_epi8(result, match8 >> 0, result, _mm512_set1_epi8((char)7));
    result = _mm512_mask_add_epi8(result, match8 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match9 = ((starts << 1) & ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)128)))));
    result = _mm512_mask_add_epi8(result, match9 >> 0, result, _mm512_set1_epi8((char)3));
    result = _mm512_mask_add_epi8(result, match9 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match10 = (~(starts << 1) & ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)129)))));
    result = _mm512_mask_add_epi8(result, match10 >> 0, result, _mm512_set1_epi8((char)18));
    result = _mm512_mask_add_epi8(result, match10 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match11 = (~(starts << 1) & ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)134)))));
    result = _mm512_mask_add_epi8(result, match11 >> 0, result, _mm512_set1_epi8((char)14));
    result = _mm512_mask_add_epi8(result, match11 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match12 =
        (~(starts << 1) &
         ((pc6 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)137)), _mm512_set1_epi8(2)) |
                  _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)147))))));
    result = _mm512_mask_add_epi8(result, match12 >> 0, result, _mm512_set1_epi8((char)13));
    result = _mm512_mask_add_epi8(result, match12 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match13 = (~(starts << 1) & ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)142)))));
    result = _mm512_mask_add_epi8(result, match13 >> 0, result, _mm512_set1_epi8((char)15));
    result = _mm512_mask_add_epi8(result, match13 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match14 = (~(starts << 1) & ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)143)))));
    result = _mm512_mask_add_epi8(result, match14 >> 0, result, _mm512_set1_epi8((char)10));
    result = _mm512_mask_add_epi8(result, match14 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match15 = (~(starts << 1) & ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)144)))));
    result = _mm512_mask_add_epi8(result, match15 >> 0, result, _mm512_set1_epi8((char)11));
    result = _mm512_mask_add_epi8(result, match15 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match16 = (~(starts << 1) & ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)148)))));
    result = _mm512_mask_add_epi8(result, match16 >> 0, result, _mm512_set1_epi8((char)15));
    result = _mm512_mask_add_epi8(result, match16 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match17 = ((starts << 1) & ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)149)))));
    result = _mm512_mask_add_epi8(result, match17 >> 0, result, _mm512_set1_epi8((char)33));
    result = _mm512_mask_add_epi8(result, match17 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match18 = (~(starts << 1) & ((pc6 & (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)150)) |
                                                  _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)156))))));
    result = _mm512_mask_add_epi8(result, match18 >> 0, result, _mm512_set1_epi8((char)19));
    result = _mm512_mask_add_epi8(result, match18 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match19 = (~(starts << 1) & ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)151)))));
    result = _mm512_mask_add_epi8(result, match19 >> 0, result, _mm512_set1_epi8((char)17));
    result = _mm512_mask_add_epi8(result, match19 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match20 = ((starts << 1) & ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)154)))));
    result = _mm512_mask_add_epi8(result, match20 >> 0, result, _mm512_set1_epi8((char)35));
    result = _mm512_mask_add_epi8(result, match20 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match21 = (~(starts << 1) & ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)157)))));
    result = _mm512_mask_add_epi8(result, match21 >> 0, result, _mm512_set1_epi8((char)21));
    result = _mm512_mask_add_epi8(result, match21 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match22 = ((starts << 1) & ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)158)))));
    result = _mm512_mask_add_epi8(result, match22 >> 0, result, _mm512_set1_epi8((char)2));
    result = _mm512_mask_add_epi8(result, match22 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match23 = (~(starts << 1) & ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)159)))));
    result = _mm512_mask_add_epi8(result, match23 >> 0, result, _mm512_set1_epi8((char)22));
    result = _mm512_mask_add_epi8(result, match23 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match24 = (~(starts << 1) & ((pc6 & ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)166)) |
                                                   _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)169))) |
                                                  _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)174))))));
    result = _mm512_mask_add_epi8(result, match24 >> 0, result, _mm512_set1_epi8((char)218));
    result = _mm512_mask_add_epi8(result, match24 >> 1, result, _mm512_set1_epi8((char)4));
    sz_u64_t match25 = (~(starts << 1) & ((pc6 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)177)),
                                                                        _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match25 >> 0, result, _mm512_set1_epi8((char)217));
    result = _mm512_mask_add_epi8(result, match25 >> 1, result, _mm512_set1_epi8((char)4));
    sz_u64_t match26 = (~(starts << 1) & ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)183)))));
    result = _mm512_mask_add_epi8(result, match26 >> 0, result, _mm512_set1_epi8((char)219));
    result = _mm512_mask_add_epi8(result, match26 >> 1, result, _mm512_set1_epi8((char)4));
    sz_u64_t match27 = ((starts << 1) & ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)191)))));
    result = _mm512_mask_add_epi8(result, match27 >> 0, result, _mm512_set1_epi8((char)248));
    result = _mm512_mask_add_epi8(result, match27 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match28 =
        (~(starts << 2) &
         (((((pe1b8 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(63)) &
                       _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))) |
             (pe1b9 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(63)) &
                       _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))))) |
            (pe1ba & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(21)) &
                       _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))) |
                      (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(31)) &
                       _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))))) |
           (pe1bb & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(63)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))))));
    result = _mm512_mask_add_epi8(result, match28 >> 0, result, _mm512_set1_epi8((char)1));
    sz_u64_t match29 =
        ((starts << 2) &
         (((((pe1b8 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(63)) &
                       _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))) |
             (pe1b9 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(63)) &
                       _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))))) |
            (pe1ba & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(21)) &
                       _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))) |
                      (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)161)), _mm512_set1_epi8(31)) &
                       _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))))) |
           (pe1bb & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(63)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))))));
    result = _mm512_mask_add_epi8(result, match29 >> 0, result, _mm512_set1_epi8((char)255));
    sz_u64_t match30 = ((starts << 2) & ((pe1ba & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)155)))));
    result = _mm512_mask_add_epi8(result, match30 >> 0, result, _mm512_set1_epi8((char)5));
    result = _mm512_mask_add_epi8(result, match30 >> 1, result, _mm512_set1_epi8((char)255));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_icelake_cyrillic_(__m512i v, sz_ptr_t target, sz_bool_t *word_start) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t alnum = ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26))) |
                      _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)48)), _mm512_set1_epi8(10)));
    sz_u64_t stop = 0;
    sz_u64_t pd0 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)208)) << 1) & continuations);
    sz_u64_t pd0_bits = pd0;
    allowed |= (pd0_bits >> 0) | (pd0_bits >> 1);
    sz_u64_t pd0_word = pd0;
    alnum |= (pd0_word >> 0) | (pd0_word >> 1);
    sz_u64_t pd1 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)209)) << 1) & continuations);
    sz_u64_t pd1_bits = pd1;
    allowed |= (pd1_bits >> 0) | (pd1_bits >> 1);
    sz_u64_t pd1_word = pd1;
    alnum |= (pd1_word >> 0) | (pd1_word >> 1);
    sz_u64_t pd2 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)210)) << 1) & continuations);
    sz_u64_t pd2_bits = pd2;
    allowed |= (pd2_bits >> 0) | (pd2_bits >> 1);
    sz_u64_t pd2_word =
        (pd2 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(2)) |
                _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)138)), _mm512_set1_epi8(54))));
    alnum |= (pd2_word >> 0) | (pd2_word >> 1);
    sz_u64_t pd3 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)211)) << 1) & continuations);
    sz_u64_t pd3_bits = pd3;
    allowed |= (pd3_bits >> 0) | (pd3_bits >> 1);
    sz_u64_t pd3_word = pd3;
    alnum |= (pd3_word >> 0) | (pd3_word >> 1);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    sz_u64_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    sz_u64_t letters = (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26)));
    __m512i result = _mm512_mask_mov_epi8(v, letters, _mm512_or_si512(v, _mm512_set1_epi8(32)));
    result = _mm512_mask_sub_epi8(result, starts & letters, result, _mm512_set1_epi8(32));
    sz_u64_t match0 = (~(starts << 1) & ((pd0 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)),
                                                                       _mm512_set1_epi8(16)))));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)16));
    result = _mm512_mask_add_epi8(result, match0 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match1 = (~(starts << 1) & ((pd0 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)),
                                                                       _mm512_set1_epi8(16)))));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)32));
    sz_u64_t match2 = (~(starts << 1) & ((pd0 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)),
                                                                       _mm512_set1_epi8(16)))));
    result = _mm512_mask_add_epi8(result, match2 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match2 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match3 = ((starts << 1) & ((pd0 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)176)),
                                                                      _mm512_set1_epi8(16)))));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)224));
    sz_u64_t match4 = ((starts << 1) & ((pd1 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)),
                                                                      _mm512_set1_epi8(16)))));
    result = _mm512_mask_add_epi8(result, match4 >> 0, result, _mm512_set1_epi8((char)32));
    result = _mm512_mask_add_epi8(result, match4 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match5 = ((starts << 1) & ((pd1 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)),
                                                                      _mm512_set1_epi8(16)))));
    result = _mm512_mask_add_epi8(result, match5 >> 0, result, _mm512_set1_epi8((char)240));
    result = _mm512_mask_add_epi8(result, match5 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match6 =
        (~(starts << 1) &
         ((((pd1 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(31)) &
                    _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))) |
            (pd2 & (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)128)) |
                    (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)138)), _mm512_set1_epi8(53)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))))) |
           (pd3 & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(13)) &
                    _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))) |
                   (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(47)) &
                    _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))))))));
    result = _mm512_mask_add_epi8(result, match6 >> 0, result, _mm512_set1_epi8((char)1));
    sz_u64_t match7 =
        ((starts << 1) &
         ((((pd1 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)161)), _mm512_set1_epi8(31)) &
                    _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))) |
            (pd2 & (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)129)) |
                    (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)139)), _mm512_set1_epi8(53)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))))) |
           (pd3 & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)130)), _mm512_set1_epi8(13)) &
                    _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))) |
                   (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)145)), _mm512_set1_epi8(47)) &
                    _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))))))));
    result = _mm512_mask_add_epi8(result, match7 >> 0, result, _mm512_set1_epi8((char)255));
    sz_u64_t match8 = (~(starts << 1) & ((pd3 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)128)))));
    result = _mm512_mask_add_epi8(result, match8 >> 0, result, _mm512_set1_epi8((char)15));
    sz_u64_t match9 = ((starts << 1) & ((pd3 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)143)))));
    result = _mm512_mask_add_epi8(result, match9 >> 0, result, _mm512_set1_epi8((char)241));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_icelake_greek_(__m512i v, sz_ptr_t target, sz_bool_t *word_start) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t alnum = ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26))) |
                      _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)48)), _mm512_set1_epi8(10)));
    sz_u64_t stop = 0;
    sz_u64_t pce = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)206)) << 1) & continuations);
    sz_u64_t pce_bits = pce;
    allowed |= (pce_bits >> 0) | (pce_bits >> 1);
    sz_u64_t pce_word =
        (pce & (((((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)134)), _mm512_set1_epi8(3)) &
                    _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))) |
                   _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)137)), _mm512_set1_epi8(2))) |
                  (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)140)), _mm512_set1_epi8(3)) &
                   _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))) |
                 _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)143)), _mm512_set1_epi8(19))) |
                _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)163)), _mm512_set1_epi8(29))));
    alnum |= (pce_word >> 0) | (pce_word >> 1);
    sz_u64_t pcf = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)207)) << 1) & continuations);
    sz_u64_t pcf_bits = pcf;
    allowed |= (pcf_bits >> 0) | (pcf_bits >> 1);
    sz_u64_t pcf_word =
        (pcf & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(54)) |
                _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)183)), _mm512_set1_epi8(9))));
    alnum |= (pcf_word >> 0) | (pcf_word >> 1);
    sz_u64_t pe1bc = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)188)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1bc_bits = pe1bc;
    allowed |= (pe1bc_bits >> 0) | (pe1bc_bits >> 1) | (pe1bc_bits >> 2);
    sz_u64_t pe1bc_word =
        (pe1bc & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(22)) |
                   _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)152)), _mm512_set1_epi8(6))) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(32))));
    alnum |= (pe1bc_word >> 0) | (pe1bc_word >> 1) | (pe1bc_word >> 2);
    sz_u64_t pe1bd = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)189)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1bd_bits = pe1bd;
    allowed |= (pe1bd_bits >> 0) | (pe1bd_bits >> 1) | (pe1bd_bits >> 2);
    sz_u64_t pe1bd_word =
        (pe1bd & ((((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(6)) |
                     _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)136)), _mm512_set1_epi8(6))) |
                    _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(8))) |
                   (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)153)), _mm512_set1_epi8(7)) &
                    _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(30))));
    alnum |= (pe1bd_word >> 0) | (pe1bd_word >> 1) | (pe1bd_word >> 2);
    sz_u64_t pe1be = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)190)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1be_bits = pe1be;
    allowed |= (pe1be_bits >> 0) | (pe1be_bits >> 1) | (pe1be_bits >> 2);
    sz_u64_t pe1be_word =
        (pe1be & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(53)) |
                   _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)182)), _mm512_set1_epi8(7))) |
                  _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)190))));
    alnum |= (pe1be_word >> 0) | (pe1be_word >> 1) | (pe1be_word >> 2);
    stop |= ((pe1be & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)190)))) >> 2;
    sz_u64_t pe1bf = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)191)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1bf_bits = pe1bf;
    allowed |= (pe1bf_bits >> 0) | (pe1bf_bits >> 1) | (pe1bf_bits >> 2);
    sz_u64_t pe1bf_word =
        (pe1bf & ((((((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)130)), _mm512_set1_epi8(3)) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)134)), _mm512_set1_epi8(7))) |
                      _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(4))) |
                     _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)150)), _mm512_set1_epi8(6))) |
                    _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(13))) |
                   _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)178)), _mm512_set1_epi8(3))) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)182)), _mm512_set1_epi8(7))));
    alnum |= (pe1bf_word >> 0) | (pe1bf_word >> 1) | (pe1bf_word >> 2);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    sz_u64_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    sz_u64_t letters = (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26)));
    __m512i result = _mm512_mask_mov_epi8(v, letters, _mm512_or_si512(v, _mm512_set1_epi8(32)));
    result = _mm512_mask_sub_epi8(result, starts & letters, result, _mm512_set1_epi8(32));
    sz_u64_t match0 = (~(starts << 1) & ((pce & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)134)))));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)38));
    sz_u64_t match1 = (~(starts << 1) & ((pce & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)136)),
                                                                       _mm512_set1_epi8(3)))));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)37));
    sz_u64_t match2 = (~(starts << 1) & ((pce & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)140)))));
    result = _mm512_mask_add_epi8(result, match2 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match3 = (~(starts << 1) & ((pce & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)142)),
                                                                       _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)255));
    result = _mm512_mask_add_epi8(result, match3 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match4 = (~(starts << 1) & ((pce & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)145)),
                                                                       _mm512_set1_epi8(15)))));
    result = _mm512_mask_add_epi8(result, match4 >> 0, result, _mm512_set1_epi8((char)32));
    sz_u64_t match5 =
        (~(starts << 1) &
         ((pce & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(2)) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)163)), _mm512_set1_epi8(9))))));
    result = _mm512_mask_add_epi8(result, match5 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match5 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match6 = ((starts << 1) & ((pce & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)172)))));
    result = _mm512_mask_add_epi8(result, match6 >> 0, result, _mm512_set1_epi8((char)218));
    sz_u64_t match7 = ((starts << 1) & ((pce & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)173)),
                                                                      _mm512_set1_epi8(3)))));
    result = _mm512_mask_add_epi8(result, match7 >> 0, result, _mm512_set1_epi8((char)219));
    sz_u64_t match8 = ((starts << 1) & ((pce & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)177)),
                                                                      _mm512_set1_epi8(15)))));
    result = _mm512_mask_add_epi8(result, match8 >> 0, result, _mm512_set1_epi8((char)224));
    sz_u64_t match9 =
        ((starts << 1) &
         ((pcf & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(2)) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)131)), _mm512_set1_epi8(9))))));
    result = _mm512_mask_add_epi8(result, match9 >> 0, result, _mm512_set1_epi8((char)32));
    result = _mm512_mask_add_epi8(result, match9 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match10 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)130)))));
    result = _mm512_mask_add_epi8(result, match10 >> 0, result, _mm512_set1_epi8((char)33));
    result = _mm512_mask_add_epi8(result, match10 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match11 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)140)))));
    result = _mm512_mask_add_epi8(result, match11 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match12 = ((starts << 1) & ((pcf & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)141)),
                                                                       _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match12 >> 0, result, _mm512_set1_epi8((char)1));
    result = _mm512_mask_add_epi8(result, match12 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match13 = (~(starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)143)))));
    result = _mm512_mask_add_epi8(result, match13 >> 0, result, _mm512_set1_epi8((char)8));
    sz_u64_t match14 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)144)))));
    result = _mm512_mask_add_epi8(result, match14 >> 0, result, _mm512_set1_epi8((char)2));
    result = _mm512_mask_add_epi8(result, match14 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match15 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)145)))));
    result = _mm512_mask_add_epi8(result, match15 >> 0, result, _mm512_set1_epi8((char)7));
    result = _mm512_mask_add_epi8(result, match15 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match16 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)149)))));
    result = _mm512_mask_add_epi8(result, match16 >> 0, result, _mm512_set1_epi8((char)17));
    result = _mm512_mask_add_epi8(result, match16 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match17 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)150)))));
    result = _mm512_mask_add_epi8(result, match17 >> 0, result, _mm512_set1_epi8((char)10));
    result = _mm512_mask_add_epi8(result, match17 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match18 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)151)))));
    result = _mm512_mask_add_epi8(result, match18 >> 0, result, _mm512_set1_epi8((char)248));
    sz_u64_t match19 =
        (~(starts << 1) &
         ((pcf & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)152)), _mm512_set1_epi8(23)) &
                    _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))) |
                   _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)183))) |
                  _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)186))))));
    result = _mm512_mask_add_epi8(result, match19 >> 0, result, _mm512_set1_epi8((char)1));
    sz_u64_t match20 =
        ((starts << 1) &
         ((pcf & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)153)), _mm512_set1_epi8(23)) &
                    _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))) |
                   _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)184))) |
                  _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)187))))));
    result = _mm512_mask_add_epi8(result, match20 >> 0, result, _mm512_set1_epi8((char)255));
    sz_u64_t match21 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)176)))));
    result = _mm512_mask_add_epi8(result, match21 >> 0, result, _mm512_set1_epi8((char)234));
    result = _mm512_mask_add_epi8(result, match21 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match22 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)177)))));
    result = _mm512_mask_add_epi8(result, match22 >> 0, result, _mm512_set1_epi8((char)240));
    result = _mm512_mask_add_epi8(result, match22 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match23 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)178)))));
    result = _mm512_mask_add_epi8(result, match23 >> 0, result, _mm512_set1_epi8((char)7));
    sz_u64_t match24 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)179)))));
    result = _mm512_mask_add_epi8(result, match24 >> 0, result, _mm512_set1_epi8((char)12));
    result = _mm512_mask_add_epi8(result, match24 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match25 = (~(starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)180)))));
    result = _mm512_mask_add_epi8(result, match25 >> 0, result, _mm512_set1_epi8((char)4));
    result = _mm512_mask_add_epi8(result, match25 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match26 = ((starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)181)))));
    result = _mm512_mask_add_epi8(result, match26 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match26 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match27 = (~(starts << 1) & ((pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)185)))));
    result = _mm512_mask_add_epi8(result, match27 >> 0, result, _mm512_set1_epi8((char)249));
    sz_u64_t match28 = (~(starts << 1) & ((pcf & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)189)),
                                                                        _mm512_set1_epi8(3)))));
    result = _mm512_mask_add_epi8(result, match28 >> 0, result, _mm512_set1_epi8((char)254));
    result = _mm512_mask_add_epi8(result, match28 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match29 =
        ((starts << 2) &
         (((((pe1bc & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(8)) |
                         _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(6))) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(8))) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)176)), _mm512_set1_epi8(8)))) |
             (pe1bd & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(6)) |
                        (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)145)), _mm512_set1_epi8(7)) &
                         _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(8))))) |
            (pe1be & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(8)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(8))) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(8))) |
                      _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)176)), _mm512_set1_epi8(2))))) |
           (pe1bf & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(2)) |
                     _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(2)))))));
    result = _mm512_mask_add_epi8(result, match29 >> 0, result, _mm512_set1_epi8((char)8));
    sz_u64_t match30 =
        (~(starts << 2) &
         (((((pe1bc & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)136)), _mm512_set1_epi8(8)) |
                         _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)152)), _mm512_set1_epi8(6))) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)168)), _mm512_set1_epi8(8))) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)184)), _mm512_set1_epi8(8)))) |
             (pe1bd & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)136)), _mm512_set1_epi8(6)) |
                        (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)153)), _mm512_set1_epi8(7)) &
                         _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)168)), _mm512_set1_epi8(8))))) |
            (pe1be & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)136)), _mm512_set1_epi8(8)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)152)), _mm512_set1_epi8(8))) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)168)), _mm512_set1_epi8(8))) |
                      _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)184)), _mm512_set1_epi8(2))))) |
           (pe1bf & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)152)), _mm512_set1_epi8(2)) |
                     _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)168)), _mm512_set1_epi8(2)))))));
    result = _mm512_mask_add_epi8(result, match30 >> 0, result, _mm512_set1_epi8((char)248));
    sz_u64_t match31 =
        ((starts << 2) &
         ((pe1bd & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)176)), _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match31 >> 0, result, _mm512_set1_epi8((char)10));
    result = _mm512_mask_add_epi8(result, match31 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match32 =
        ((starts << 2) &
         ((pe1bd & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)178)), _mm512_set1_epi8(4)))));
    result = _mm512_mask_add_epi8(result, match32 >> 0, result, _mm512_set1_epi8((char)214));
    result = _mm512_mask_add_epi8(result, match32 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match33 =
        ((starts << 2) &
         ((pe1bd & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)182)), _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match33 >> 0, result, _mm512_set1_epi8((char)228));
    result = _mm512_mask_add_epi8(result, match33 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match34 =
        ((starts << 2) &
         ((pe1bd & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)184)), _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match34 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match35 =
        ((starts << 2) &
         ((pe1bd & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)186)), _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match35 >> 0, result, _mm512_set1_epi8((char)240));
    result = _mm512_mask_add_epi8(result, match35 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match36 =
        ((starts << 2) &
         ((pe1bd & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)188)), _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match36 >> 0, result, _mm512_set1_epi8((char)254));
    result = _mm512_mask_add_epi8(result, match36 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match37 = ((starts << 2) & (((pe1be & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)179))) |
                                          (pe1bf & (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)131)) |
                                                    _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)179)))))));
    result = _mm512_mask_add_epi8(result, match37 >> 0, result, _mm512_set1_epi8((char)9));
    sz_u64_t match38 =
        (~(starts << 2) &
         ((pe1be & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)186)), _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match38 >> 0, result, _mm512_set1_epi8((char)246));
    result = _mm512_mask_add_epi8(result, match38 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match39 = (~(starts << 2) & (((pe1be & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)188))) |
                                           (pe1bf & (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)140)) |
                                                     _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)188)))))));
    result = _mm512_mask_add_epi8(result, match39 >> 0, result, _mm512_set1_epi8((char)247));
    sz_u64_t match40 =
        (~(starts << 2) &
         ((pe1bf & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)136)), _mm512_set1_epi8(4)))));
    result = _mm512_mask_add_epi8(result, match40 >> 0, result, _mm512_set1_epi8((char)42));
    result = _mm512_mask_add_epi8(result, match40 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match41 =
        (~(starts << 2) &
         ((pe1bf & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)154)), _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match41 >> 0, result, _mm512_set1_epi8((char)28));
    result = _mm512_mask_add_epi8(result, match41 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match42 = ((starts << 2) & ((pe1bf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)165)))));
    result = _mm512_mask_add_epi8(result, match42 >> 0, result, _mm512_set1_epi8((char)7));
    sz_u64_t match43 =
        (~(starts << 2) &
         ((pe1bf & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)170)), _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match43 >> 0, result, _mm512_set1_epi8((char)16));
    result = _mm512_mask_add_epi8(result, match43 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match44 = (~(starts << 2) & ((pe1bf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)172)))));
    result = _mm512_mask_add_epi8(result, match44 >> 0, result, _mm512_set1_epi8((char)249));
    sz_u64_t match45 =
        (~(starts << 2) &
         ((pe1bf & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)184)), _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match45 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match46 =
        (~(starts << 2) &
         ((pe1bf & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)186)), _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match46 >> 0, result, _mm512_set1_epi8((char)2));
    result = _mm512_mask_add_epi8(result, match46 >> 1, result, _mm512_set1_epi8((char)254));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_icelake_georgian_(__m512i v, sz_ptr_t target, sz_bool_t *word_start) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t alnum = ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26))) |
                      _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)48)), _mm512_set1_epi8(10)));
    sz_u64_t stop = 0;
    sz_u64_t pe182 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)130)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe182_bits = pe182;
    allowed |= (pe182_bits >> 0) | (pe182_bits >> 1) | (pe182_bits >> 2);
    sz_u64_t pe182_word =
        (pe182 & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(2)) |
                    (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)142)), _mm512_set1_epi8(3)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))) |
                   _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)145)), _mm512_set1_epi8(9))) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(32))));
    alnum |= (pe182_word >> 0) | (pe182_word >> 1) | (pe182_word >> 2);
    sz_u64_t pe183 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)131)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe183_bits = pe183;
    allowed |= (pe183_bits >> 0) | (pe183_bits >> 1) | (pe183_bits >> 2);
    sz_u64_t pe183_word =
        (pe183 & ((((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(6)) |
                     _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)135))) |
                    _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)141))) |
                   _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(43))) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)188)), _mm512_set1_epi8(4))));
    alnum |= (pe183_word >> 0) | (pe183_word >> 1) | (pe183_word >> 2);
    sz_u64_t pe1b2 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)178)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1b2_bits = pe1b2;
    allowed |= (pe1b2_bits >> 0) | (pe1b2_bits >> 1) | (pe1b2_bits >> 2);
    sz_u64_t pe1b2_word =
        (pe1b2 & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(11)) |
                   _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(43))) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)189)), _mm512_set1_epi8(3))));
    alnum |= (pe1b2_word >> 0) | (pe1b2_word >> 1) | (pe1b2_word >> 2);
    stop |= ((pe1b2 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(8)))) >>
            2;
    sz_u64_t pe1b3 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)179)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1b3_bits = pe1b3;
    allowed |= (pe1b3_bits >> 0) | (pe1b3_bits >> 1) | (pe1b3_bits >> 2);
    sz_u64_t pe1b3_word =
        (pe1b3 & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)169)), _mm512_set1_epi8(4)) |
                    _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)174)), _mm512_set1_epi8(6))) |
                   _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)181)), _mm512_set1_epi8(2))) |
                  _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)186))));
    alnum |= (pe1b3_word >> 0) | (pe1b3_word >> 1) | (pe1b3_word >> 2);
    sz_u64_t pe2b4 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)180)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)226)) << 2)) &
                      continuations);
    sz_u64_t pe2b4_bits = pe2b4;
    allowed |= (pe2b4_bits >> 0) | (pe2b4_bits >> 1) | (pe2b4_bits >> 2);
    sz_u64_t pe2b4_word =
        (pe2b4 & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(38)) |
                    _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)167))) |
                   _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)173))) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)176)), _mm512_set1_epi8(16))));
    alnum |= (pe2b4_word >> 0) | (pe2b4_word >> 1) | (pe2b4_word >> 2);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    sz_u64_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    sz_u64_t letters = (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26)));
    __m512i result = _mm512_mask_mov_epi8(v, letters, _mm512_or_si512(v, _mm512_set1_epi8(32)));
    result = _mm512_mask_sub_epi8(result, starts & letters, result, _mm512_set1_epi8(32));
    sz_u64_t match0 =
        (~(starts << 2) &
         ((pe182 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(32)))));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match0 >> 1, result, _mm512_set1_epi8((char)50));
    result = _mm512_mask_add_epi8(result, match0 >> 2, result, _mm512_set1_epi8((char)1));
    sz_u64_t match1 =
        (~(starts << 2) &
         ((pe183 & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(6)) |
                     _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)135))) |
                    _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)141))))));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)32));
    result = _mm512_mask_add_epi8(result, match1 >> 1, result, _mm512_set1_epi8((char)49));
    result = _mm512_mask_add_epi8(result, match1 >> 2, result, _mm512_set1_epi8((char)1));
    sz_u64_t match2 =
        ((starts << 2) &
         ((pe183 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(43)) |
                    _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)189)), _mm512_set1_epi8(3))))));
    result = _mm512_mask_add_epi8(result, match2 >> 1, result, _mm512_set1_epi8((char)47));
    sz_u64_t match3 = ((starts << 2) & ((pe1b2 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)136)))));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)2));
    result = _mm512_mask_add_epi8(result, match3 >> 1, result, _mm512_set1_epi8((char)231));
    result = _mm512_mask_add_epi8(result, match3 >> 2, result, _mm512_set1_epi8((char)9));
    sz_u64_t match4 = (~(starts << 2) & ((pe1b2 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)137)))));
    result = _mm512_mask_add_epi8(result, match4 >> 0, result, _mm512_set1_epi8((char)1));
    sz_u64_t match5 = ((starts << 2) & ((pe1b2 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)138)))));
    result = _mm512_mask_add_epi8(result, match5 >> 0, result, _mm512_set1_epi8((char)255));
    sz_u64_t match6 =
        (~(starts << 2) &
         ((pe1b2 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(43)) |
                    _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)189)), _mm512_set1_epi8(3))))));
    result = _mm512_mask_add_epi8(result, match6 >> 1, result, _mm512_set1_epi8((char)209));
    sz_u64_t match7 = ((starts << 2) & ((pe2b4 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)),
                                                                        _mm512_set1_epi8(32)))));
    result = _mm512_mask_add_epi8(result, match7 >> 0, result, _mm512_set1_epi8((char)32));
    result = _mm512_mask_add_epi8(result, match7 >> 1, result, _mm512_set1_epi8((char)206));
    result = _mm512_mask_add_epi8(result, match7 >> 2, result, _mm512_set1_epi8((char)255));
    sz_u64_t match8 =
        ((starts << 2) &
         ((pe2b4 & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(6)) |
                     _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)167))) |
                    _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)173))))));
    result = _mm512_mask_add_epi8(result, match8 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match8 >> 1, result, _mm512_set1_epi8((char)207));
    result = _mm512_mask_add_epi8(result, match8 >> 2, result, _mm512_set1_epi8((char)255));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_icelake_armenian_(__m512i v, sz_ptr_t target, sz_bool_t *word_start) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t alnum = ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26))) |
                      _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)48)), _mm512_set1_epi8(10)));
    sz_u64_t stop = 0;
    sz_u64_t pd4 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)212)) << 1) & continuations);
    sz_u64_t pd4_bits = pd4;
    allowed |= (pd4_bits >> 0) | (pd4_bits >> 1);
    sz_u64_t pd4_word =
        (pd4 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(48)) |
                _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)177)), _mm512_set1_epi8(15))));
    alnum |= (pd4_word >> 0) | (pd4_word >> 1);
    sz_u64_t pd5 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)213)) << 1) & continuations);
    sz_u64_t pd5_bits = pd5;
    allowed |= (pd5_bits >> 0) | (pd5_bits >> 1);
    sz_u64_t pd5_word =
        (pd5 & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(23)) |
                 _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)153))) |
                _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(32))));
    alnum |= (pd5_word >> 0) | (pd5_word >> 1);
    sz_u64_t pd6 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)214)) << 1) & continuations);
    sz_u64_t pd6_bits = pd6;
    allowed |= (pd6_bits >> 0) | (pd6_bits >> 1);
    sz_u64_t pd6_word = (pd6 &
                         _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(9)));
    alnum |= (pd6_word >> 0) | (pd6_word >> 1);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    sz_u64_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    sz_u64_t letters = (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26)));
    __m512i result = _mm512_mask_mov_epi8(v, letters, _mm512_or_si512(v, _mm512_set1_epi8(32)));
    result = _mm512_mask_sub_epi8(result, starts & letters, result, _mm512_set1_epi8(32));
    sz_u64_t match0 =
        (~(starts << 1) &
         ((pd4 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(47)) &
                  _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))))));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)1));
    sz_u64_t match1 =
        ((starts << 1) &
         ((pd4 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(47)) &
                  _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))))));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)255));
    sz_u64_t match2 =
        (~(starts << 1) &
         (((pd4 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)177)), _mm512_set1_epi8(15))) |
           (pd5 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(7))))));
    result = _mm512_mask_add_epi8(result, match2 >> 0, result, _mm512_set1_epi8((char)240));
    result = _mm512_mask_add_epi8(result, match2 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match3 = (~(starts << 1) & ((pd5 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)),
                                                                       _mm512_set1_epi8(16)))));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)48));
    sz_u64_t match4 =
        ((starts << 1) &
         (((pd5 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)161)), _mm512_set1_epi8(15))) |
           (pd6 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(7))))));
    result = _mm512_mask_add_epi8(result, match4 >> 0, result, _mm512_set1_epi8((char)16));
    result = _mm512_mask_add_epi8(result, match4 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match5 = ((starts << 1) & ((pd5 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)176)),
                                                                      _mm512_set1_epi8(16)))));
    result = _mm512_mask_add_epi8(result, match5 >> 0, result, _mm512_set1_epi8((char)208));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_icelake_fullwidth_(__m512i v, sz_ptr_t target, sz_bool_t *word_start) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t alnum = ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26))) |
                      _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)48)), _mm512_set1_epi8(10)));
    sz_u64_t stop = 0;
    sz_u64_t pefbc = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)188)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)239)) << 2)) &
                      continuations);
    sz_u64_t pefbc_bits = pefbc;
    allowed |= (pefbc_bits >> 0) | (pefbc_bits >> 1) | (pefbc_bits >> 2);
    sz_u64_t pefbc_word =
        (pefbc & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(10)) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)161)), _mm512_set1_epi8(26))));
    alnum |= (pefbc_word >> 0) | (pefbc_word >> 1) | (pefbc_word >> 2);
    sz_u64_t pefbd = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)189)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)239)) << 2)) &
                      continuations);
    sz_u64_t pefbd_bits = pefbd;
    allowed |= (pefbd_bits >> 0) | (pefbd_bits >> 1) | (pefbd_bits >> 2);
    sz_u64_t pefbd_word =
        (pefbd & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(26)) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)166)), _mm512_set1_epi8(26))));
    alnum |= (pefbd_word >> 0) | (pefbd_word >> 1) | (pefbd_word >> 2);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    sz_u64_t starts = alnum & ~((alnum << 1) | (*word_start ? 0 : 1));
    *word_start = (sz_bool_t) !((alnum >> (length - 1)) & 1);
    sz_u64_t letters = (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)65)), _mm512_set1_epi8(26)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)97)), _mm512_set1_epi8(26)));
    __m512i result = _mm512_mask_mov_epi8(v, letters, _mm512_or_si512(v, _mm512_set1_epi8(32)));
    result = _mm512_mask_sub_epi8(result, starts & letters, result, _mm512_set1_epi8(32));
    sz_u64_t match0 =
        (~(starts << 2) &
         ((pefbc & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)161)), _mm512_set1_epi8(26)))));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match0 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match1 = ((starts << 2) & ((pefbd & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)),
                                                                        _mm512_set1_epi8(26)))));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)32));
    result = _mm512_mask_add_epi8(result, match1 >> 1, result, _mm512_set1_epi8((char)255));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
#endif

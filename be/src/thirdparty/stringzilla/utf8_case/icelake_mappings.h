/* Generated Unicode 17 byte transforms. See UNICODE-LICENSE.txt. */
#ifndef STRINGZILLA_UTF8_CASE_ICELAKE_MAPPINGS_H_
#define STRINGZILLA_UTF8_CASE_ICELAKE_MAPPINGS_H_
SZ_HELPER_AUTO sz_size_t sz_utf8_lower_icelake_greek_basic_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pce = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)206)) << 1) & continuations);
    sz_u64_t pce_bits = pce;
    allowed |= (pce_bits >> 0) | (pce_bits >> 1);
    stop |= ((pce & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)163)))) >> 1;
    sz_u64_t pcf = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)207)) << 1) & continuations);
    sz_u64_t pcf_bits = pcf;
    allowed |= (pcf_bits >> 0) | (pcf_bits >> 1);
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_false_k);
    sz_u64_t match0 = (pce & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)134)));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)38));
    sz_u64_t match1 = (pce &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)136)), _mm512_set1_epi8(3)));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)37));
    sz_u64_t match2 = (pce & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)140)));
    result = _mm512_mask_add_epi8(result, match2 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match3 = (pce &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)142)), _mm512_set1_epi8(2)));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)255));
    result = _mm512_mask_add_epi8(result, match3 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match4 = (pce &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)145)), _mm512_set1_epi8(15)));
    result = _mm512_mask_add_epi8(result, match4 >> 0, result, _mm512_set1_epi8((char)32));
    sz_u64_t match5 = (pce &
                       (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(2)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)164)), _mm512_set1_epi8(8))));
    result = _mm512_mask_add_epi8(result, match5 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match5 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match6 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)143)));
    result = _mm512_mask_add_epi8(result, match6 >> 0, result, _mm512_set1_epi8((char)8));
    sz_u64_t match7 =
        (pcf & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)152)), _mm512_set1_epi8(23)) &
                  _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))) |
                 _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)183))) |
                _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)186))));
    result = _mm512_mask_add_epi8(result, match7 >> 0, result, _mm512_set1_epi8((char)1));
    sz_u64_t match8 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)180)));
    result = _mm512_mask_add_epi8(result, match8 >> 0, result, _mm512_set1_epi8((char)4));
    result = _mm512_mask_add_epi8(result, match8 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match9 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)185)));
    result = _mm512_mask_add_epi8(result, match9 >> 0, result, _mm512_set1_epi8((char)249));
    sz_u64_t match10 = (pcf &
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)189)), _mm512_set1_epi8(3)));
    result = _mm512_mask_add_epi8(result, match10 >> 0, result, _mm512_set1_epi8((char)254));
    result = _mm512_mask_add_epi8(result, match10 >> 1, result, _mm512_set1_epi8((char)254));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_lower_icelake_latin1_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pc2 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)194)) << 1) & continuations);
    sz_u64_t pc2_bits = pc2;
    allowed |= (pc2_bits >> 0) | (pc2_bits >> 1);
    sz_u64_t pc3 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)195)) << 1) & continuations);
    sz_u64_t pc3_bits = pc3;
    allowed |= (pc3_bits >> 0) | (pc3_bits >> 1);
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_false_k);
    sz_u64_t match0 = (pc3 &
                       (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(23)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)152)), _mm512_set1_epi8(7))));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)32));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_lower_icelake_latin_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pc2 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)194)) << 1) & continuations);
    sz_u64_t pc2_bits = pc2;
    allowed |= (pc2_bits >> 0) | (pc2_bits >> 1);
    sz_u64_t pc3 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)195)) << 1) & continuations);
    sz_u64_t pc3_bits = pc3;
    allowed |= (pc3_bits >> 0) | (pc3_bits >> 1);
    sz_u64_t pc4 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)196)) << 1) & continuations);
    sz_u64_t pc4_bits = pc4;
    allowed |= (pc4_bits >> 0) | (pc4_bits >> 1);
    stop |= ((pc4 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)176)))) >> 1;
    sz_u64_t pc5 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)197)) << 1) & continuations);
    sz_u64_t pc5_bits = pc5;
    allowed |= (pc5_bits >> 0) | (pc5_bits >> 1);
    sz_u64_t pc6 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)198)) << 1) & continuations);
    sz_u64_t pc6_bits = pc6;
    allowed |= (pc6_bits >> 0) | (pc6_bits >> 1);
    sz_u64_t pe1b8 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)184)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1b8_bits = pe1b8;
    allowed |= (pe1b8_bits >> 0) | (pe1b8_bits >> 1) | (pe1b8_bits >> 2);
    sz_u64_t pe1b9 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)185)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1b9_bits = pe1b9;
    allowed |= (pe1b9_bits >> 0) | (pe1b9_bits >> 1) | (pe1b9_bits >> 2);
    sz_u64_t pe1ba = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)186)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1ba_bits = pe1ba;
    allowed |= (pe1ba_bits >> 0) | (pe1ba_bits >> 1) | (pe1ba_bits >> 2);
    stop |= ((pe1ba & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)158)))) >> 2;
    sz_u64_t pe1bb = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)187)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1bb_bits = pe1bb;
    allowed |= (pe1bb_bits >> 0) | (pe1bb_bits >> 1) | (pe1bb_bits >> 2);
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_false_k);
    sz_u64_t match0 = (pc3 &
                       (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(23)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)152)), _mm512_set1_epi8(7))));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)32));
    sz_u64_t match1 =
        (((pc4 & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(47)) &
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
           _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)188)))));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)1));
    sz_u64_t match2 = (pc4 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)191)));
    result = _mm512_mask_add_epi8(result, match2 >> 0, result, _mm512_set1_epi8((char)193));
    result = _mm512_mask_add_epi8(result, match2 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match3 = (pc5 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)184)));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)7));
    result = _mm512_mask_add_epi8(result, match3 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match4 = (pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)129)));
    result = _mm512_mask_add_epi8(result, match4 >> 0, result, _mm512_set1_epi8((char)18));
    result = _mm512_mask_add_epi8(result, match4 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match5 = (pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)134)));
    result = _mm512_mask_add_epi8(result, match5 >> 0, result, _mm512_set1_epi8((char)14));
    result = _mm512_mask_add_epi8(result, match5 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match6 = (pc6 &
                       (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)137)), _mm512_set1_epi8(2)) |
                        _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)147))));
    result = _mm512_mask_add_epi8(result, match6 >> 0, result, _mm512_set1_epi8((char)13));
    result = _mm512_mask_add_epi8(result, match6 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match7 = (pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)142)));
    result = _mm512_mask_add_epi8(result, match7 >> 0, result, _mm512_set1_epi8((char)15));
    result = _mm512_mask_add_epi8(result, match7 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match8 = (pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)143)));
    result = _mm512_mask_add_epi8(result, match8 >> 0, result, _mm512_set1_epi8((char)10));
    result = _mm512_mask_add_epi8(result, match8 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match9 = (pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)144)));
    result = _mm512_mask_add_epi8(result, match9 >> 0, result, _mm512_set1_epi8((char)11));
    result = _mm512_mask_add_epi8(result, match9 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match10 = (pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)148)));
    result = _mm512_mask_add_epi8(result, match10 >> 0, result, _mm512_set1_epi8((char)15));
    result = _mm512_mask_add_epi8(result, match10 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match11 = (pc6 & (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)150)) |
                               _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)156))));
    result = _mm512_mask_add_epi8(result, match11 >> 0, result, _mm512_set1_epi8((char)19));
    result = _mm512_mask_add_epi8(result, match11 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match12 = (pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)151)));
    result = _mm512_mask_add_epi8(result, match12 >> 0, result, _mm512_set1_epi8((char)17));
    result = _mm512_mask_add_epi8(result, match12 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match13 = (pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)157)));
    result = _mm512_mask_add_epi8(result, match13 >> 0, result, _mm512_set1_epi8((char)21));
    result = _mm512_mask_add_epi8(result, match13 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match14 = (pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)159)));
    result = _mm512_mask_add_epi8(result, match14 >> 0, result, _mm512_set1_epi8((char)22));
    result = _mm512_mask_add_epi8(result, match14 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match15 = (pc6 & ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)166)) |
                                _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)169))) |
                               _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)174))));
    result = _mm512_mask_add_epi8(result, match15 >> 0, result, _mm512_set1_epi8((char)218));
    result = _mm512_mask_add_epi8(result, match15 >> 1, result, _mm512_set1_epi8((char)4));
    sz_u64_t match16 = (pc6 &
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)177)), _mm512_set1_epi8(2)));
    result = _mm512_mask_add_epi8(result, match16 >> 0, result, _mm512_set1_epi8((char)217));
    result = _mm512_mask_add_epi8(result, match16 >> 1, result, _mm512_set1_epi8((char)4));
    sz_u64_t match17 = (pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)183)));
    result = _mm512_mask_add_epi8(result, match17 >> 0, result, _mm512_set1_epi8((char)219));
    result = _mm512_mask_add_epi8(result, match17 >> 1, result, _mm512_set1_epi8((char)4));
    sz_u64_t match18 =
        ((((pe1b8 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(63)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))) |
           (pe1b9 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(63)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))))) |
          (pe1ba & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(21)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))) |
                    (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(31)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))))) |
         (pe1bb & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(63)) &
                   _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))));
    result = _mm512_mask_add_epi8(result, match18 >> 0, result, _mm512_set1_epi8((char)1));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_lower_icelake_cyrillic_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pd0 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)208)) << 1) & continuations);
    sz_u64_t pd0_bits = pd0;
    allowed |= (pd0_bits >> 0) | (pd0_bits >> 1);
    sz_u64_t pd1 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)209)) << 1) & continuations);
    sz_u64_t pd1_bits = pd1;
    allowed |= (pd1_bits >> 0) | (pd1_bits >> 1);
    sz_u64_t pd2 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)210)) << 1) & continuations);
    sz_u64_t pd2_bits = pd2;
    allowed |= (pd2_bits >> 0) | (pd2_bits >> 1);
    sz_u64_t pd3 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)211)) << 1) & continuations);
    sz_u64_t pd3_bits = pd3;
    allowed |= (pd3_bits >> 0) | (pd3_bits >> 1);
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_false_k);
    sz_u64_t match0 = (pd0 &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(16)));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)16));
    result = _mm512_mask_add_epi8(result, match0 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match1 = (pd0 &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(16)));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)32));
    sz_u64_t match2 = (pd0 &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(16)));
    result = _mm512_mask_add_epi8(result, match2 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match2 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match3 =
        (((pd1 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(31)) &
                  _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))) |
          (pd2 & (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)128)) |
                  (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)138)), _mm512_set1_epi8(53)) &
                   _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))))) |
         (pd3 & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(13)) &
                  _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))) |
                 (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(47)) &
                  _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))))));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)1));
    sz_u64_t match4 = (pd3 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)128)));
    result = _mm512_mask_add_epi8(result, match4 >> 0, result, _mm512_set1_epi8((char)15));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_lower_icelake_greek_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pce = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)206)) << 1) & continuations);
    sz_u64_t pce_bits = pce;
    allowed |= (pce_bits >> 0) | (pce_bits >> 1);
    stop |= ((pce & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)163)))) >> 1;
    sz_u64_t pcf = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)207)) << 1) & continuations);
    sz_u64_t pcf_bits = pcf;
    allowed |= (pcf_bits >> 0) | (pcf_bits >> 1);
    sz_u64_t pe1bc = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)188)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1bc_bits = pe1bc;
    allowed |= (pe1bc_bits >> 0) | (pe1bc_bits >> 1) | (pe1bc_bits >> 2);
    sz_u64_t pe1bd = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)189)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1bd_bits = pe1bd;
    allowed |= (pe1bd_bits >> 0) | (pe1bd_bits >> 1) | (pe1bd_bits >> 2);
    sz_u64_t pe1be = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)190)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1be_bits = pe1be;
    allowed |= (pe1be_bits >> 0) | (pe1be_bits >> 1) | (pe1be_bits >> 2);
    sz_u64_t pe1bf = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)191)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1bf_bits = pe1bf;
    allowed |= (pe1bf_bits >> 0) | (pe1bf_bits >> 1) | (pe1bf_bits >> 2);
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_false_k);
    sz_u64_t match0 = (pce & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)134)));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)38));
    sz_u64_t match1 = (pce &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)136)), _mm512_set1_epi8(3)));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)37));
    sz_u64_t match2 = (pce & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)140)));
    result = _mm512_mask_add_epi8(result, match2 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match3 = (pce &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)142)), _mm512_set1_epi8(2)));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)255));
    result = _mm512_mask_add_epi8(result, match3 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match4 = (pce &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)145)), _mm512_set1_epi8(15)));
    result = _mm512_mask_add_epi8(result, match4 >> 0, result, _mm512_set1_epi8((char)32));
    sz_u64_t match5 = (pce &
                       (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(2)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)164)), _mm512_set1_epi8(8))));
    result = _mm512_mask_add_epi8(result, match5 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match5 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match6 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)143)));
    result = _mm512_mask_add_epi8(result, match6 >> 0, result, _mm512_set1_epi8((char)8));
    sz_u64_t match7 =
        (pcf & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)152)), _mm512_set1_epi8(23)) &
                  _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))) |
                 _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)183))) |
                _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)186))));
    result = _mm512_mask_add_epi8(result, match7 >> 0, result, _mm512_set1_epi8((char)1));
    sz_u64_t match8 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)180)));
    result = _mm512_mask_add_epi8(result, match8 >> 0, result, _mm512_set1_epi8((char)4));
    result = _mm512_mask_add_epi8(result, match8 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match9 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)185)));
    result = _mm512_mask_add_epi8(result, match9 >> 0, result, _mm512_set1_epi8((char)249));
    sz_u64_t match10 = (pcf &
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)189)), _mm512_set1_epi8(3)));
    result = _mm512_mask_add_epi8(result, match10 >> 0, result, _mm512_set1_epi8((char)254));
    result = _mm512_mask_add_epi8(result, match10 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match11 =
        ((((pe1bc & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)136)), _mm512_set1_epi8(8)) |
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
                   _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)168)), _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match11 >> 0, result, _mm512_set1_epi8((char)248));
    sz_u64_t match12 = (pe1be &
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)186)), _mm512_set1_epi8(2)));
    result = _mm512_mask_add_epi8(result, match12 >> 0, result, _mm512_set1_epi8((char)246));
    result = _mm512_mask_add_epi8(result, match12 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match13 = ((pe1be & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)188))) |
                        (pe1bf & (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)140)) |
                                  _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)188)))));
    result = _mm512_mask_add_epi8(result, match13 >> 0, result, _mm512_set1_epi8((char)247));
    sz_u64_t match14 = (pe1bf &
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)136)), _mm512_set1_epi8(4)));
    result = _mm512_mask_add_epi8(result, match14 >> 0, result, _mm512_set1_epi8((char)42));
    result = _mm512_mask_add_epi8(result, match14 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match15 = (pe1bf &
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)154)), _mm512_set1_epi8(2)));
    result = _mm512_mask_add_epi8(result, match15 >> 0, result, _mm512_set1_epi8((char)28));
    result = _mm512_mask_add_epi8(result, match15 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match16 = (pe1bf &
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)170)), _mm512_set1_epi8(2)));
    result = _mm512_mask_add_epi8(result, match16 >> 0, result, _mm512_set1_epi8((char)16));
    result = _mm512_mask_add_epi8(result, match16 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match17 = (pe1bf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)172)));
    result = _mm512_mask_add_epi8(result, match17 >> 0, result, _mm512_set1_epi8((char)249));
    sz_u64_t match18 = (pe1bf &
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)184)), _mm512_set1_epi8(2)));
    result = _mm512_mask_add_epi8(result, match18 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match19 = (pe1bf &
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)186)), _mm512_set1_epi8(2)));
    result = _mm512_mask_add_epi8(result, match19 >> 0, result, _mm512_set1_epi8((char)2));
    result = _mm512_mask_add_epi8(result, match19 >> 1, result, _mm512_set1_epi8((char)254));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_lower_icelake_georgian_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pe182 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)130)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe182_bits = pe182;
    allowed |= (pe182_bits >> 0) | (pe182_bits >> 1) | (pe182_bits >> 2);
    sz_u64_t pe183 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)131)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe183_bits = pe183;
    allowed |= (pe183_bits >> 0) | (pe183_bits >> 1) | (pe183_bits >> 2);
    sz_u64_t pe1b2 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)178)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1b2_bits = pe1b2;
    allowed |= (pe1b2_bits >> 0) | (pe1b2_bits >> 1) | (pe1b2_bits >> 2);
    sz_u64_t pe1b3 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)179)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1b3_bits = pe1b3;
    allowed |= (pe1b3_bits >> 0) | (pe1b3_bits >> 1) | (pe1b3_bits >> 2);
    sz_u64_t pe2b4 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)180)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)226)) << 2)) &
                      continuations);
    sz_u64_t pe2b4_bits = pe2b4;
    allowed |= (pe2b4_bits >> 0) | (pe2b4_bits >> 1) | (pe2b4_bits >> 2);
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_false_k);
    sz_u64_t match0 = (pe182 &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(32)));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match0 >> 1, result, _mm512_set1_epi8((char)50));
    result = _mm512_mask_add_epi8(result, match0 >> 2, result, _mm512_set1_epi8((char)1));
    sz_u64_t match1 = (pe183 &
                       ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(6)) |
                         _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)135))) |
                        _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)141))));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)32));
    result = _mm512_mask_add_epi8(result, match1 >> 1, result, _mm512_set1_epi8((char)49));
    result = _mm512_mask_add_epi8(result, match1 >> 2, result, _mm512_set1_epi8((char)1));
    sz_u64_t match2 = (pe1b2 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)137)));
    result = _mm512_mask_add_epi8(result, match2 >> 0, result, _mm512_set1_epi8((char)1));
    sz_u64_t match3 = (pe1b2 &
                       (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(43)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)189)), _mm512_set1_epi8(3))));
    result = _mm512_mask_add_epi8(result, match3 >> 1, result, _mm512_set1_epi8((char)209));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_lower_icelake_armenian_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pd4 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)212)) << 1) & continuations);
    sz_u64_t pd4_bits = pd4;
    allowed |= (pd4_bits >> 0) | (pd4_bits >> 1);
    sz_u64_t pd5 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)213)) << 1) & continuations);
    sz_u64_t pd5_bits = pd5;
    allowed |= (pd5_bits >> 0) | (pd5_bits >> 1);
    sz_u64_t pd6 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)214)) << 1) & continuations);
    sz_u64_t pd6_bits = pd6;
    allowed |= (pd6_bits >> 0) | (pd6_bits >> 1);
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_false_k);
    sz_u64_t match0 = (pd4 &
                       (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(47)) &
                        _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)1));
    sz_u64_t match1 =
        ((pd4 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)177)), _mm512_set1_epi8(15))) |
         (pd5 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(7))));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)240));
    result = _mm512_mask_add_epi8(result, match1 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match2 = (pd5 &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(16)));
    result = _mm512_mask_add_epi8(result, match2 >> 0, result, _mm512_set1_epi8((char)48));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_lower_icelake_fullwidth_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pefbc = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)188)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)239)) << 2)) &
                      continuations);
    sz_u64_t pefbc_bits = pefbc;
    allowed |= (pefbc_bits >> 0) | (pefbc_bits >> 1) | (pefbc_bits >> 2);
    sz_u64_t pefbd = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)189)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)239)) << 2)) &
                      continuations);
    sz_u64_t pefbd_bits = pefbd;
    allowed |= (pefbd_bits >> 0) | (pefbd_bits >> 1) | (pefbd_bits >> 2);
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_false_k);
    sz_u64_t match0 = (pefbc &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)161)), _mm512_set1_epi8(26)));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match0 >> 1, result, _mm512_set1_epi8((char)1));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_upper_icelake_greek_basic_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pce = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)206)) << 1) & continuations);
    sz_u64_t pce_bits = pce;
    allowed |= (pce_bits >> 0) | (pce_bits >> 1);
    stop |= ((pce & (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)144)) |
                     _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)176))))) >>
            1;
    sz_u64_t pcf = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)207)) << 1) & continuations);
    sz_u64_t pcf_bits = pcf;
    allowed |= (pcf_bits >> 0) | (pcf_bits >> 1);
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_true_k);
    sz_u64_t match0 = (pce & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)172)));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)218));
    sz_u64_t match1 = (pce &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)173)), _mm512_set1_epi8(3)));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)219));
    sz_u64_t match2 = (pce &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)177)), _mm512_set1_epi8(15)));
    result = _mm512_mask_add_epi8(result, match2 >> 0, result, _mm512_set1_epi8((char)224));
    sz_u64_t match3 = (pcf &
                       (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(2)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)131)), _mm512_set1_epi8(9))));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)32));
    result = _mm512_mask_add_epi8(result, match3 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match4 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)130)));
    result = _mm512_mask_add_epi8(result, match4 >> 0, result, _mm512_set1_epi8((char)33));
    result = _mm512_mask_add_epi8(result, match4 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match5 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)140)));
    result = _mm512_mask_add_epi8(result, match5 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match6 = (pcf &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)141)), _mm512_set1_epi8(2)));
    result = _mm512_mask_add_epi8(result, match6 >> 0, result, _mm512_set1_epi8((char)1));
    result = _mm512_mask_add_epi8(result, match6 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match7 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)144)));
    result = _mm512_mask_add_epi8(result, match7 >> 0, result, _mm512_set1_epi8((char)2));
    result = _mm512_mask_add_epi8(result, match7 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match8 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)145)));
    result = _mm512_mask_add_epi8(result, match8 >> 0, result, _mm512_set1_epi8((char)7));
    result = _mm512_mask_add_epi8(result, match8 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match9 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)149)));
    result = _mm512_mask_add_epi8(result, match9 >> 0, result, _mm512_set1_epi8((char)17));
    result = _mm512_mask_add_epi8(result, match9 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match10 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)150)));
    result = _mm512_mask_add_epi8(result, match10 >> 0, result, _mm512_set1_epi8((char)10));
    result = _mm512_mask_add_epi8(result, match10 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match11 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)151)));
    result = _mm512_mask_add_epi8(result, match11 >> 0, result, _mm512_set1_epi8((char)248));
    sz_u64_t match12 =
        (pcf & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)153)), _mm512_set1_epi8(23)) &
                  _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))) |
                 _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)184))) |
                _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)187))));
    result = _mm512_mask_add_epi8(result, match12 >> 0, result, _mm512_set1_epi8((char)255));
    sz_u64_t match13 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)176)));
    result = _mm512_mask_add_epi8(result, match13 >> 0, result, _mm512_set1_epi8((char)234));
    result = _mm512_mask_add_epi8(result, match13 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match14 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)177)));
    result = _mm512_mask_add_epi8(result, match14 >> 0, result, _mm512_set1_epi8((char)240));
    result = _mm512_mask_add_epi8(result, match14 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match15 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)178)));
    result = _mm512_mask_add_epi8(result, match15 >> 0, result, _mm512_set1_epi8((char)7));
    sz_u64_t match16 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)179)));
    result = _mm512_mask_add_epi8(result, match16 >> 0, result, _mm512_set1_epi8((char)12));
    result = _mm512_mask_add_epi8(result, match16 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match17 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)181)));
    result = _mm512_mask_add_epi8(result, match17 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match17 >> 1, result, _mm512_set1_epi8((char)255));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_upper_icelake_latin1_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pc2 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)194)) << 1) & continuations);
    sz_u64_t pc2_bits = pc2;
    allowed |= (pc2_bits >> 0) | (pc2_bits >> 1);
    sz_u64_t pc3 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)195)) << 1) & continuations);
    sz_u64_t pc3_bits = pc3;
    allowed |= (pc3_bits >> 0) | (pc3_bits >> 1);
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_true_k);
    sz_u64_t match0 = (pc2 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)181)));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)231));
    result = _mm512_mask_add_epi8(result, match0 >> 1, result, _mm512_set1_epi8((char)12));
    sz_u64_t match1 = (pc3 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)159)));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)180));
    result = _mm512_mask_add_epi8(result, match1 >> 1, result, _mm512_set1_epi8((char)144));
    sz_u64_t match2 = (pc3 &
                       (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(23)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)184)), _mm512_set1_epi8(7))));
    result = _mm512_mask_add_epi8(result, match2 >> 0, result, _mm512_set1_epi8((char)224));
    sz_u64_t match3 = (pc3 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)191)));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)249));
    result = _mm512_mask_add_epi8(result, match3 >> 1, result, _mm512_set1_epi8((char)2));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_upper_icelake_latin_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pc2 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)194)) << 1) & continuations);
    sz_u64_t pc2_bits = pc2;
    allowed |= (pc2_bits >> 0) | (pc2_bits >> 1);
    sz_u64_t pc3 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)195)) << 1) & continuations);
    sz_u64_t pc3_bits = pc3;
    allowed |= (pc3_bits >> 0) | (pc3_bits >> 1);
    sz_u64_t pc4 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)196)) << 1) & continuations);
    sz_u64_t pc4_bits = pc4;
    allowed |= (pc4_bits >> 0) | (pc4_bits >> 1);
    stop |= ((pc4 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)177)))) >> 1;
    sz_u64_t pc5 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)197)) << 1) & continuations);
    sz_u64_t pc5_bits = pc5;
    allowed |= (pc5_bits >> 0) | (pc5_bits >> 1);
    stop |= ((pc5 & (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)137)) |
                     _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)191))))) >>
            1;
    sz_u64_t pc6 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)198)) << 1) & continuations);
    sz_u64_t pc6_bits = pc6;
    allowed |= (pc6_bits >> 0) | (pc6_bits >> 1);
    stop |= ((pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)155)))) >> 1;
    sz_u64_t pe1b8 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)184)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1b8_bits = pe1b8;
    allowed |= (pe1b8_bits >> 0) | (pe1b8_bits >> 1) | (pe1b8_bits >> 2);
    sz_u64_t pe1b9 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)185)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1b9_bits = pe1b9;
    allowed |= (pe1b9_bits >> 0) | (pe1b9_bits >> 1) | (pe1b9_bits >> 2);
    sz_u64_t pe1ba = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)186)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1ba_bits = pe1ba;
    allowed |= (pe1ba_bits >> 0) | (pe1ba_bits >> 1) | (pe1ba_bits >> 2);
    sz_u64_t pe1bb = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)187)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1bb_bits = pe1bb;
    allowed |= (pe1bb_bits >> 0) | (pe1bb_bits >> 1) | (pe1bb_bits >> 2);
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_true_k);
    sz_u64_t match0 = (pc2 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)181)));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)231));
    result = _mm512_mask_add_epi8(result, match0 >> 1, result, _mm512_set1_epi8((char)12));
    sz_u64_t match1 = (pc3 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)159)));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)180));
    result = _mm512_mask_add_epi8(result, match1 >> 1, result, _mm512_set1_epi8((char)144));
    sz_u64_t match2 = (pc3 &
                       (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(23)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)184)), _mm512_set1_epi8(7))));
    result = _mm512_mask_add_epi8(result, match2 >> 0, result, _mm512_set1_epi8((char)224));
    sz_u64_t match3 = (pc3 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)191)));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)249));
    result = _mm512_mask_add_epi8(result, match3 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match4 =
        (((pc4 & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(47)) &
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
           _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)189)))));
    result = _mm512_mask_add_epi8(result, match4 >> 0, result, _mm512_set1_epi8((char)255));
    sz_u64_t match5 = (pc5 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)128)));
    result = _mm512_mask_add_epi8(result, match5 >> 0, result, _mm512_set1_epi8((char)63));
    result = _mm512_mask_add_epi8(result, match5 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match6 = (pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)128)));
    result = _mm512_mask_add_epi8(result, match6 >> 0, result, _mm512_set1_epi8((char)3));
    result = _mm512_mask_add_epi8(result, match6 >> 1, result, _mm512_set1_epi8((char)3));
    sz_u64_t match7 = (pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)149)));
    result = _mm512_mask_add_epi8(result, match7 >> 0, result, _mm512_set1_epi8((char)33));
    result = _mm512_mask_add_epi8(result, match7 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match8 = (pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)154)));
    result = _mm512_mask_add_epi8(result, match8 >> 0, result, _mm512_set1_epi8((char)35));
    result = _mm512_mask_add_epi8(result, match8 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match9 = (pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)158)));
    result = _mm512_mask_add_epi8(result, match9 >> 0, result, _mm512_set1_epi8((char)2));
    result = _mm512_mask_add_epi8(result, match9 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match10 = (pc6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)191)));
    result = _mm512_mask_add_epi8(result, match10 >> 0, result, _mm512_set1_epi8((char)248));
    result = _mm512_mask_add_epi8(result, match10 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match11 =
        ((((pe1b8 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(63)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))) |
           (pe1b9 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(63)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))))) |
          (pe1ba & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(21)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))) |
                    (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)161)), _mm512_set1_epi8(31)) &
                     _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))))) |
         (pe1bb & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(63)) &
                   _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))));
    result = _mm512_mask_add_epi8(result, match11 >> 0, result, _mm512_set1_epi8((char)255));
    sz_u64_t match12 = (pe1ba & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)150)));
    result = _mm512_mask_add_epi8(result, match12 >> 0, result, _mm512_set1_epi8((char)27));
    result = _mm512_mask_add_epi8(result, match12 >> 1, result, _mm512_set1_epi8((char)18));
    result = _mm512_mask_add_epi8(result, match12 >> 2, result, _mm512_set1_epi8((char)103));
    sz_u64_t match13 = (pe1ba & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)151)));
    result = _mm512_mask_add_epi8(result, match13 >> 0, result, _mm512_set1_epi8((char)241));
    result = _mm512_mask_add_epi8(result, match13 >> 1, result, _mm512_set1_epi8((char)18));
    result = _mm512_mask_add_epi8(result, match13 >> 2, result, _mm512_set1_epi8((char)115));
    sz_u64_t match14 = (pe1ba & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)152)));
    result = _mm512_mask_add_epi8(result, match14 >> 0, result, _mm512_set1_epi8((char)242));
    result = _mm512_mask_add_epi8(result, match14 >> 1, result, _mm512_set1_epi8((char)18));
    result = _mm512_mask_add_epi8(result, match14 >> 2, result, _mm512_set1_epi8((char)118));
    sz_u64_t match15 = (pe1ba & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)153)));
    result = _mm512_mask_add_epi8(result, match15 >> 0, result, _mm512_set1_epi8((char)241));
    result = _mm512_mask_add_epi8(result, match15 >> 1, result, _mm512_set1_epi8((char)18));
    result = _mm512_mask_add_epi8(result, match15 >> 2, result, _mm512_set1_epi8((char)120));
    sz_u64_t match16 = (pe1ba & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)154)));
    result = _mm512_mask_add_epi8(result, match16 >> 0, result, _mm512_set1_epi8((char)36));
    result = _mm512_mask_add_epi8(result, match16 >> 1, result, _mm512_set1_epi8((char)16));
    result = _mm512_mask_add_epi8(result, match16 >> 2, result, _mm512_set1_epi8((char)96));
    sz_u64_t match17 = (pe1ba & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)155)));
    result = _mm512_mask_add_epi8(result, match17 >> 0, result, _mm512_set1_epi8((char)5));
    result = _mm512_mask_add_epi8(result, match17 >> 1, result, _mm512_set1_epi8((char)255));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_upper_icelake_cyrillic_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pd0 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)208)) << 1) & continuations);
    sz_u64_t pd0_bits = pd0;
    allowed |= (pd0_bits >> 0) | (pd0_bits >> 1);
    sz_u64_t pd1 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)209)) << 1) & continuations);
    sz_u64_t pd1_bits = pd1;
    allowed |= (pd1_bits >> 0) | (pd1_bits >> 1);
    sz_u64_t pd2 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)210)) << 1) & continuations);
    sz_u64_t pd2_bits = pd2;
    allowed |= (pd2_bits >> 0) | (pd2_bits >> 1);
    sz_u64_t pd3 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)211)) << 1) & continuations);
    sz_u64_t pd3_bits = pd3;
    allowed |= (pd3_bits >> 0) | (pd3_bits >> 1);
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_true_k);
    sz_u64_t match0 = (pd0 &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)176)), _mm512_set1_epi8(16)));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)224));
    sz_u64_t match1 = (pd1 &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(16)));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)32));
    result = _mm512_mask_add_epi8(result, match1 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match2 = (pd1 &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(16)));
    result = _mm512_mask_add_epi8(result, match2 >> 0, result, _mm512_set1_epi8((char)240));
    result = _mm512_mask_add_epi8(result, match2 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match3 =
        (((pd1 & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)161)), _mm512_set1_epi8(31)) &
                  _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))) |
          (pd2 & (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)129)) |
                  (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)139)), _mm512_set1_epi8(53)) &
                   _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))))) |
         (pd3 & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)130)), _mm512_set1_epi8(13)) &
                  _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))) |
                 (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)145)), _mm512_set1_epi8(47)) &
                  _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))))));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)255));
    sz_u64_t match4 = (pd3 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)143)));
    result = _mm512_mask_add_epi8(result, match4 >> 0, result, _mm512_set1_epi8((char)241));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_upper_icelake_greek_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pce = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)206)) << 1) & continuations);
    sz_u64_t pce_bits = pce;
    allowed |= (pce_bits >> 0) | (pce_bits >> 1);
    stop |= ((pce & (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)144)) |
                     _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)176))))) >>
            1;
    sz_u64_t pcf = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)207)) << 1) & continuations);
    sz_u64_t pcf_bits = pcf;
    allowed |= (pcf_bits >> 0) | (pcf_bits >> 1);
    sz_u64_t pe1bc = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)188)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1bc_bits = pe1bc;
    allowed |= (pe1bc_bits >> 0) | (pe1bc_bits >> 1) | (pe1bc_bits >> 2);
    sz_u64_t pe1bd = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)189)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1bd_bits = pe1bd;
    allowed |= (pe1bd_bits >> 0) | (pe1bd_bits >> 1) | (pe1bd_bits >> 2);
    stop |= ((pe1bd & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(7)) &
                       _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0))))) >>
            2;
    sz_u64_t pe1be = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)190)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1be_bits = pe1be;
    allowed |= (pe1be_bits >> 0) | (pe1be_bits >> 1) | (pe1be_bits >> 2);
    stop |= ((pe1be & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(48)) |
                         _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)178)), _mm512_set1_epi8(3))) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)182)), _mm512_set1_epi8(2))) |
                       (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)188)), _mm512_set1_epi8(3)) &
                        _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(0)))))) >>
            2;
    sz_u64_t pe1bf = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)191)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1bf_bits = pe1bf;
    allowed |= (pe1bf_bits >> 0) | (pe1bf_bits >> 1) | (pe1bf_bits >> 2);
    stop |= ((pe1bf &
              (((((((((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)130)), _mm512_set1_epi8(3)) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)134)), _mm512_set1_epi8(2))) |
                      _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)140))) |
                     _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)146)), _mm512_set1_epi8(2))) |
                    _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)150)), _mm512_set1_epi8(2))) |
                   _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)162)), _mm512_set1_epi8(3))) |
                  _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)166)), _mm512_set1_epi8(2))) |
                 _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)178)), _mm512_set1_epi8(3))) |
                _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)182)), _mm512_set1_epi8(2))) |
               _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)188))))) >>
            2;
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_true_k);
    sz_u64_t match0 = (pce & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)172)));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)218));
    sz_u64_t match1 = (pce &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)173)), _mm512_set1_epi8(3)));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)219));
    sz_u64_t match2 = (pce &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)177)), _mm512_set1_epi8(15)));
    result = _mm512_mask_add_epi8(result, match2 >> 0, result, _mm512_set1_epi8((char)224));
    sz_u64_t match3 = (pcf &
                       (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(2)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)131)), _mm512_set1_epi8(9))));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)32));
    result = _mm512_mask_add_epi8(result, match3 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match4 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)130)));
    result = _mm512_mask_add_epi8(result, match4 >> 0, result, _mm512_set1_epi8((char)33));
    result = _mm512_mask_add_epi8(result, match4 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match5 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)140)));
    result = _mm512_mask_add_epi8(result, match5 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match6 = (pcf &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)141)), _mm512_set1_epi8(2)));
    result = _mm512_mask_add_epi8(result, match6 >> 0, result, _mm512_set1_epi8((char)1));
    result = _mm512_mask_add_epi8(result, match6 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match7 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)144)));
    result = _mm512_mask_add_epi8(result, match7 >> 0, result, _mm512_set1_epi8((char)2));
    result = _mm512_mask_add_epi8(result, match7 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match8 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)145)));
    result = _mm512_mask_add_epi8(result, match8 >> 0, result, _mm512_set1_epi8((char)7));
    result = _mm512_mask_add_epi8(result, match8 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match9 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)149)));
    result = _mm512_mask_add_epi8(result, match9 >> 0, result, _mm512_set1_epi8((char)17));
    result = _mm512_mask_add_epi8(result, match9 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match10 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)150)));
    result = _mm512_mask_add_epi8(result, match10 >> 0, result, _mm512_set1_epi8((char)10));
    result = _mm512_mask_add_epi8(result, match10 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match11 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)151)));
    result = _mm512_mask_add_epi8(result, match11 >> 0, result, _mm512_set1_epi8((char)248));
    sz_u64_t match12 =
        (pcf & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)153)), _mm512_set1_epi8(23)) &
                  _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))) |
                 _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)184))) |
                _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)187))));
    result = _mm512_mask_add_epi8(result, match12 >> 0, result, _mm512_set1_epi8((char)255));
    sz_u64_t match13 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)176)));
    result = _mm512_mask_add_epi8(result, match13 >> 0, result, _mm512_set1_epi8((char)234));
    result = _mm512_mask_add_epi8(result, match13 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match14 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)177)));
    result = _mm512_mask_add_epi8(result, match14 >> 0, result, _mm512_set1_epi8((char)240));
    result = _mm512_mask_add_epi8(result, match14 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match15 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)178)));
    result = _mm512_mask_add_epi8(result, match15 >> 0, result, _mm512_set1_epi8((char)7));
    sz_u64_t match16 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)179)));
    result = _mm512_mask_add_epi8(result, match16 >> 0, result, _mm512_set1_epi8((char)12));
    result = _mm512_mask_add_epi8(result, match16 >> 1, result, _mm512_set1_epi8((char)254));
    sz_u64_t match17 = (pcf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)181)));
    result = _mm512_mask_add_epi8(result, match17 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match17 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match18 =
        ((((pe1bc & (((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(8)) |
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(6))) |
                      _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(8))) |
                     _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)176)), _mm512_set1_epi8(8)))) |
           (pe1bd & ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(6)) |
                      (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)145)), _mm512_set1_epi8(7)) &
                       _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1)))) |
                     _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(8))))) |
          (pe1be & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)176)), _mm512_set1_epi8(2)))) |
         (pe1bf & (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(2)) |
                   _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(2)))));
    result = _mm512_mask_add_epi8(result, match18 >> 0, result, _mm512_set1_epi8((char)8));
    sz_u64_t match19 = (pe1bd &
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)176)), _mm512_set1_epi8(2)));
    result = _mm512_mask_add_epi8(result, match19 >> 0, result, _mm512_set1_epi8((char)10));
    result = _mm512_mask_add_epi8(result, match19 >> 1, result, _mm512_set1_epi8((char)1));
    sz_u64_t match20 = (pe1bd &
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)178)), _mm512_set1_epi8(4)));
    result = _mm512_mask_add_epi8(result, match20 >> 0, result, _mm512_set1_epi8((char)214));
    result = _mm512_mask_add_epi8(result, match20 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match21 = (pe1bd &
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)182)), _mm512_set1_epi8(2)));
    result = _mm512_mask_add_epi8(result, match21 >> 0, result, _mm512_set1_epi8((char)228));
    result = _mm512_mask_add_epi8(result, match21 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match22 = (pe1bd &
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)184)), _mm512_set1_epi8(2)));
    result = _mm512_mask_add_epi8(result, match22 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match23 = (pe1bd &
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)186)), _mm512_set1_epi8(2)));
    result = _mm512_mask_add_epi8(result, match23 >> 0, result, _mm512_set1_epi8((char)240));
    result = _mm512_mask_add_epi8(result, match23 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match24 = (pe1bd &
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)188)), _mm512_set1_epi8(2)));
    result = _mm512_mask_add_epi8(result, match24 >> 0, result, _mm512_set1_epi8((char)254));
    result = _mm512_mask_add_epi8(result, match24 >> 1, result, _mm512_set1_epi8((char)2));
    sz_u64_t match25 = (pe1bf & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)165)));
    result = _mm512_mask_add_epi8(result, match25 >> 0, result, _mm512_set1_epi8((char)7));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_upper_icelake_georgian_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pe182 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)130)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe182_bits = pe182;
    allowed |= (pe182_bits >> 0) | (pe182_bits >> 1) | (pe182_bits >> 2);
    sz_u64_t pe183 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)131)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe183_bits = pe183;
    allowed |= (pe183_bits >> 0) | (pe183_bits >> 1) | (pe183_bits >> 2);
    sz_u64_t pe1b2 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)178)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1b2_bits = pe1b2;
    allowed |= (pe1b2_bits >> 0) | (pe1b2_bits >> 1) | (pe1b2_bits >> 2);
    stop |= ((pe1b2 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(8)))) >>
            2;
    sz_u64_t pe1b3 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)179)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)225)) << 2)) &
                      continuations);
    sz_u64_t pe1b3_bits = pe1b3;
    allowed |= (pe1b3_bits >> 0) | (pe1b3_bits >> 1) | (pe1b3_bits >> 2);
    sz_u64_t pe2b4 = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)180)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)226)) << 2)) &
                      continuations);
    sz_u64_t pe2b4_bits = pe2b4;
    allowed |= (pe2b4_bits >> 0) | (pe2b4_bits >> 1) | (pe2b4_bits >> 2);
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_true_k);
    sz_u64_t match0 = (pe183 &
                       (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)144)), _mm512_set1_epi8(43)) |
                        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)189)), _mm512_set1_epi8(3))));
    result = _mm512_mask_add_epi8(result, match0 >> 1, result, _mm512_set1_epi8((char)47));
    sz_u64_t match1 = (pe1b2 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)136)));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)2));
    result = _mm512_mask_add_epi8(result, match1 >> 1, result, _mm512_set1_epi8((char)231));
    result = _mm512_mask_add_epi8(result, match1 >> 2, result, _mm512_set1_epi8((char)9));
    sz_u64_t match2 = (pe1b2 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)138)));
    result = _mm512_mask_add_epi8(result, match2 >> 0, result, _mm512_set1_epi8((char)255));
    sz_u64_t match3 = (pe2b4 &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(32)));
    result = _mm512_mask_add_epi8(result, match3 >> 0, result, _mm512_set1_epi8((char)32));
    result = _mm512_mask_add_epi8(result, match3 >> 1, result, _mm512_set1_epi8((char)206));
    result = _mm512_mask_add_epi8(result, match3 >> 2, result, _mm512_set1_epi8((char)255));
    sz_u64_t match4 = (pe2b4 &
                       ((_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)160)), _mm512_set1_epi8(6)) |
                         _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)167))) |
                        _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)173))));
    result = _mm512_mask_add_epi8(result, match4 >> 0, result, _mm512_set1_epi8((char)224));
    result = _mm512_mask_add_epi8(result, match4 >> 1, result, _mm512_set1_epi8((char)207));
    result = _mm512_mask_add_epi8(result, match4 >> 2, result, _mm512_set1_epi8((char)255));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_upper_icelake_armenian_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pd4 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)212)) << 1) & continuations);
    sz_u64_t pd4_bits = pd4;
    allowed |= (pd4_bits >> 0) | (pd4_bits >> 1);
    sz_u64_t pd5 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)213)) << 1) & continuations);
    sz_u64_t pd5_bits = pd5;
    allowed |= (pd5_bits >> 0) | (pd5_bits >> 1);
    sz_u64_t pd6 = ((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)214)) << 1) & continuations);
    sz_u64_t pd6_bits = pd6;
    allowed |= (pd6_bits >> 0) | (pd6_bits >> 1);
    stop |= ((pd6 & _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)135)))) >> 1;
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_true_k);
    sz_u64_t match0 = (pd4 &
                       (_mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(47)) &
                        _mm512_cmpeq_epi8_mask(_mm512_and_si512(v, _mm512_set1_epi8(1)), _mm512_set1_epi8(1))));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)255));
    sz_u64_t match1 =
        ((pd5 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)161)), _mm512_set1_epi8(15))) |
         (pd6 & _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)128)), _mm512_set1_epi8(7))));
    result = _mm512_mask_add_epi8(result, match1 >> 0, result, _mm512_set1_epi8((char)16));
    result = _mm512_mask_add_epi8(result, match1 >> 1, result, _mm512_set1_epi8((char)255));
    sz_u64_t match2 = (pd5 &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)176)), _mm512_set1_epi8(16)));
    result = _mm512_mask_add_epi8(result, match2 >> 0, result, _mm512_set1_epi8((char)208));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
SZ_HELPER_AUTO sz_size_t sz_utf8_upper_icelake_fullwidth_(__m512i v, sz_ptr_t target, sz_size_t available) {
    sz_u64_t continuations = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)),
                                                    _mm512_set1_epi8(0x40));
    sz_u64_t allowed = ~(sz_u64_t)_mm512_movepi8_mask(v);
    sz_u64_t stop = 0;
    sz_u64_t pefbc = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)188)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)239)) << 2)) &
                      continuations);
    sz_u64_t pefbc_bits = pefbc;
    allowed |= (pefbc_bits >> 0) | (pefbc_bits >> 1) | (pefbc_bits >> 2);
    sz_u64_t pefbd = (((_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)189)) << 1) &
                       (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)239)) << 2)) &
                      continuations);
    sz_u64_t pefbd_bits = pefbd;
    allowed |= (pefbd_bits >> 0) | (pefbd_bits >> 1) | (pefbd_bits >> 2);
    stop |= ~allowed | ~sz_u64_mask_until_(available);
    stop |= ~allowed;
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    __m512i result = sz_utf8_case_icelake_ascii_(v, sz_true_k);
    sz_u64_t match0 = (pefbd &
                       _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)129)), _mm512_set1_epi8(26)));
    result = _mm512_mask_add_epi8(result, match0 >> 0, result, _mm512_set1_epi8((char)32));
    result = _mm512_mask_add_epi8(result, match0 >> 1, result, _mm512_set1_epi8((char)255));
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), result);
    return length;
}
#endif

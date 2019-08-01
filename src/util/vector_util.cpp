#include "util/vector_util.h"

#include <immintrin.h>

#include "util/bit_util.h"
#include "util/math_util.h"

namespace tpl::util {

void VectorUtil::SelectionVectorToByteVector(const u32 n,
                                             const sel_t *RESTRICT sel_vector,
                                             u8 *RESTRICT byte_vector) {
  for (u32 i = 0; i < n; i++) {
    byte_vector[sel_vector[i]] = 0xff;
  }
}

// TODO(pmenon): Consider splitting into dense and sparse implementations.
void VectorUtil::ByteVectorToSelectionVector(const u32 n,
                                             const u8 *RESTRICT byte_vector,
                                             sel_t *RESTRICT sel_vector,
                                             u32 *RESTRICT size) {
  // Byte-vector index
  u32 i = 0;

  // Selection vector write index
  u32 k = 0;

  // Main vector loop
  const auto eight = _mm_set1_epi16(8);
  auto idx = _mm_set1_epi16(0);
  for (; i + 8 <= n; i += 8) {
    const auto word = *reinterpret_cast<const u64 *>(byte_vector + i);
    const auto mask = _pext_u64(word, 0x202020202020202);
    TPL_ASSERT(mask < 256, "Out-of-bounds mask");
    const auto match_pos_scaled = _mm_loadl_epi64(
        reinterpret_cast<const __m128i *>(&simd::k8BitMatchLUT[mask]));
    const auto match_pos = _mm_cvtepi8_epi16(match_pos_scaled);
    const auto pos_vec = _mm_add_epi16(idx, match_pos);
    idx = _mm_add_epi16(idx, eight);
    _mm_storeu_si128(reinterpret_cast<__m128i *>(sel_vector + k), pos_vec);
    k += BitUtil::CountPopulation(static_cast<u32>(mask));
  }

  // Tail
  for (; i < n; i++) {
    sel_vector[k] = i;
    k += static_cast<u32>(byte_vector[i] == 0xFF);
  }

  *size = k;
}

void VectorUtil::ByteVectorToBitVector(const u32 n,
                                       const u8 *RESTRICT byte_vector,
                                       u64 *RESTRICT bit_vector) {
  // Byte-vector index
  u32 i = 0;

  // Bit-vector word index
  u32 k = 0;

  // Main vector loop
  for (; i + 64 <= n; i += 64, k++) {
    const auto v_lo =
        _mm256_loadu_si256(reinterpret_cast<const __m256i *>(byte_vector + i));
    const auto v_hi = _mm256_loadu_si256(
        reinterpret_cast<const __m256i *>(byte_vector + i + 32));
    const auto hi = static_cast<u32>(_mm256_movemask_epi8(v_hi));
    const auto lo = static_cast<u32>(_mm256_movemask_epi8(v_lo));
    bit_vector[k] = (static_cast<u64>(hi) << 32u) | lo;
  }

  // Tail
  for (; i < n; i++) {
    const auto val = static_cast<i8>(byte_vector[i]);
    const auto mask = static_cast<u64>(1) << (i % 64u);
    bit_vector[k] ^= (static_cast<u64>(val) ^ bit_vector[k]) & mask;
  }
}

void VectorUtil::ByteVectorAnd(const u32 n, const u8 *RESTRICT byte_vector_1,
                               u8 *RESTRICT byte_vector_2) {
  for (u32 i = 0; i < n; i++) {
    byte_vector_2[i] &= byte_vector_1[i];
  }
}

// TODO(pmenon): Consider splitting into dense and sparse implementations.
void VectorUtil::BitVectorToSelectionVector(const u32 n,
                                            const u64 *RESTRICT bit_vector,
                                            sel_t *RESTRICT sel_vector,
                                            u32 *RESTRICT size) {
  const u32 num_words = MathUtil::DivRoundUp(n, 64);
  
  u32 k = 0;
  for (u32 i = 0; i < num_words; i++) {
    u64 word = bit_vector[i];
    while (word != 0) {
      const u64 t = word & -word;
      const u32 r = BitUtil::CountTrailingZeros(word);
      sel_vector[k++] = i * 64 + r;
      word ^= t;
    }
  }
  *size = k;
}

}  // namespace tpl::util

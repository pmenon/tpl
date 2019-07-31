#include "util/vector_util.h"

#include <immintrin.h>

namespace tpl::util {

void VectorUtil::SelectionVectorToByteVector(const u32 n,
                                             const sel_t *RESTRICT sel_vector,
                                             u8 *RESTRICT byte_vector) {
  for (u32 i = 0; i < n; i++) {
    byte_vector[sel_vector[i]] = 0xff;
  }
}

void VectorUtil::ByteVectorToSelectionVector(u32 n,
                                             const u8 *RESTRICT byte_vector,
                                             sel_t *RESTRICT sel_vector,
                                             u32 *size) {}

void VectorUtil::ByteVectorToBitVector(u32 n, const u8 *RESTRICT byte_vector,
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
    const i8 val = static_cast<i8>(byte_vector[i]);
    u64 mask = static_cast<u64>(1) << (i % 64u);
    bit_vector[k] ^= (static_cast<u64>(val) ^ bit_vector[k]) & mask;
  }
}

void VectorUtil::ByteVectorAnd(u32 n, const u8 *RESTRICT byte_vector_1,
                               u8 *RESTRICT byte_vector_2) {
  for (u32 i = 0; i < n; i++) {
    byte_vector_2[i] &= byte_vector_1[i];
  }
}

}  // namespace tpl::util

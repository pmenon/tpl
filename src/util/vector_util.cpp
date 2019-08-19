#include "util/vector_util.h"

#include <immintrin.h>

#include "util/bit_util.h"
#include "util/bit_vector.h"
#include "util/math_util.h"
#include "util/simd/types.h"

namespace tpl::util {

u32 VectorUtil::IntersectSelected(const sel_t *sel_vector_1,
                                  const u32 sel_vector_1_len,
                                  const sel_t *sel_vector_2,
                                  const u32 sel_vector_2_len,
                                  sel_t *out_sel_vector) {
  // No-op if either vector is empty
  if (sel_vector_1_len == 0 || sel_vector_2_len == 0) {
    return 0;
  }

  // Canonical-ize; ensure the first vector is smaller than the second
  if (sel_vector_1_len > sel_vector_2_len) {
    return IntersectSelected(sel_vector_2, sel_vector_2_len, sel_vector_1,
                             sel_vector_1_len, out_sel_vector);
  }

  // Run

  // TODO(pmenon): We can use something faster or adaptive, like Katsov et.al or
  //               Inoue et. al, but there isn't a need for fast sorted-set
  //               intersection at the moment.

  u32 i = 0, j = 0, k = 0;
  while (i < sel_vector_1_len && j < sel_vector_2_len) {
    if (sel_vector_1[i] == sel_vector_2[j]) {
      out_sel_vector[k++] = sel_vector_1[i];
      i++;
      j++;
    } else if (sel_vector_1[i] < sel_vector_2[j]) {
      i++;
    } else {
      j++;
    }
  }

  return k;
}

u32 VectorUtil::IntersectSelected(const sel_t *sel_vector,
                                  const u32 sel_vector_len,
                                  const u64 *bit_vector,
                                  const u32 bit_vector_len,
                                  sel_t *out_sel_vector) {
  BitVector bv(const_cast<u64 *>(bit_vector), bit_vector_len);

  u32 k = 0;
  for (u32 i = 0; i < sel_vector_len; i++) {
    const auto index = sel_vector[i];
    out_sel_vector[k] = index;
    k += bv.Test(index);
  }
  return k;
}

u32 VectorUtil::DiffSelected_Scalar(const u32 n, const sel_t *sel_vector,
                                    const u32 m, sel_t *out_sel_vector) {
  u32 i = 0, j = 0, k = 0;
  for (; i < m; i++, j++) {
    while (j < sel_vector[i]) {
      out_sel_vector[k++] = j++;
    }
  }
  while (j < n) {
    out_sel_vector[k++] = j++;
  }

  return n - m;
}

u32 VectorUtil::DiffSelected_WithScratchPad(const u32 n,
                                            const sel_t *sel_vector,
                                            const u32 sel_vector_len,
                                            sel_t *out_sel_vector,
                                            u8 *scratch) {
  TPL_ASSERT(n <= kDefaultVectorSize, "Selection vector too large");
  std::memset(scratch, 0, n);
  VectorUtil::SelectionVectorToByteVector(sel_vector_len, sel_vector, scratch);
  for (u32 i = 0; i < n; i++) {
    scratch[i] = ~scratch[i];
  }
  return VectorUtil::ByteVectorToSelectionVector(n, scratch, out_sel_vector);
}

u32 VectorUtil::DiffSelected(const u32 n, const sel_t *sel_vector,
                             const u32 sel_vector_len, sel_t *out_sel_vector) {
  u8 scratch[kDefaultVectorSize];
  return DiffSelected_WithScratchPad(n, sel_vector, sel_vector_len,
                                     out_sel_vector, scratch);
}

void VectorUtil::SelectionVectorToByteVector(const u32 n,
                                             const sel_t *RESTRICT sel_vector,
                                             u8 *RESTRICT byte_vector) {
  for (u32 i = 0; i < n; i++) {
    byte_vector[sel_vector[i]] = 0xff;
  }
}

// TODO(pmenon): Consider splitting into dense and sparse implementations.
u32 VectorUtil::ByteVectorToSelectionVector(const u32 n, const u8 *byte_vector,
                                            sel_t *sel_vector) {
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

  return k;
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

void VectorUtil::BitVectorToByteVector(u32 n, const u64 *bit_vector,
                                       u8 *byte_vector) {
  const __m256i shuffle =
      _mm256_setr_epi64x(0x0000000000000000, 0x0101010101010101,
                         0x0202020202020202, 0x0303030303030303);
  const __m256i bit_mask = _mm256_set1_epi64x(0x7fbfdfeff7fbfdfe);

  // Byte-vector write index
  u32 k = 0;

  // Main vector loop processes 64 elements per iteration
  for (u32 i = 0; i < n / 64; i++, k += 64) {
    u64 word = bit_vector[i];

    // Lower 32-bits first
    __m256i vmask = _mm256_set1_epi32(static_cast<u32>(word));
    vmask = _mm256_shuffle_epi8(vmask, shuffle);
    vmask = _mm256_or_si256(vmask, bit_mask);
    __m256i vbytes = _mm256_cmpeq_epi8(vmask, _mm256_set1_epi64x(-1));
    _mm256_storeu_si256(reinterpret_cast<__m256i *>(byte_vector + k), vbytes);

    // Upper 32-bits
    vmask = _mm256_set1_epi32(static_cast<i32>(word >> 32u));
    vmask = _mm256_shuffle_epi8(vmask, shuffle);
    vmask = _mm256_or_si256(vmask, bit_mask);
    vbytes = _mm256_cmpeq_epi8(vmask, _mm256_set1_epi64x(-1));
    _mm256_storeu_si256(reinterpret_cast<__m256i *>(byte_vector + k + 32),
                        vbytes);
  }

  // Process last word in scalar loop
  if (auto tail_size = n % 64; tail_size != 0) {
    u64 word = bit_vector[n / 64];
    for (u32 i = 0; i < tail_size; i++, k++) {
      byte_vector[k] = -((word & 0x1ull) == 1);
      word >>= 1u;
    }
  }
}

// TODO(pmenon): Consider splitting into dense and sparse implementations.
u32 VectorUtil::BitVectorToSelectionVector(const u32 n, const u64 *bit_vector,
                                           sel_t *sel_vector) {
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
  return k;
}

}  // namespace tpl::util

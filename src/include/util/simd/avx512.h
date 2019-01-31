#pragma once

#include <immintrin.h>

#include "util/common.h"
#include "util/macros.h"
#include "util/simd/types.h"

namespace tpl::util::simd {

#define USE_GATHER 1

/**
 * A 512-bit SIMD register vector. This is a purely internal class that holds
 * common functions for other user-visible vector classes
 */
class Vec512b {
 public:
  Vec512b() = default;
  Vec512b(const __m512i &reg) : reg_(reg) {}

  // Type-cast operator so that Vec*'s can be used directly with intrinsics
  ALWAYS_INLINE operator __m512i() const { return reg_; }

 protected:
  const __m512i &reg() const { return reg_; }

 protected:
  __m512i reg_;
};

class Vec8 : public Vec512b {
 public:
  Vec8() = default;
  Vec8(i64 val) noexcept { reg_ = _mm512_set1_epi64(val); }
  Vec8(const __m512i &reg) noexcept : Vec512b(reg) {}
  Vec8(i64 val1, i64 val2, i64 val3, i64 val4, i64 val5, i64 val6, i64 val7,
       i64 val8) noexcept {
    reg_ = _mm512_setr_epi64(val1, val2, val3, val4, val5, val6, val7, val8);
  }

  template <typename T>
  void Load(const T *ptr);

  template <typename T>
  void Gather(const T *ptr, const Vec8 &pos) {
#if USE_GATHER
    reg_ = _mm512_i64gather_epi64(pos, ptr, 8);
#else
    alignas(64) i64 x[Size()];
    pos.Store(x);
    reg_ = _mm512_setr_epi64(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]],
                             ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]]);
#endif
  }

  ALWAYS_INLINE void Store(i64 *ptr) const { _mm512_storeu_si512(ptr, reg()); }

  ALWAYS_INLINE i64 Extract(u32 index) const {
    TPL_ASSERT(index < 8, "Out-of-bounds mask element access");
    alignas(64) i64 x[Size()];
    Store(x);
    return x[index & 7];
  }

  ALWAYS_INLINE i64 operator[](u32 index) const { return Extract(index); }

  ALWAYS_INLINE static constexpr u32 Size() { return 8; }
};

class Vec16 : public Vec512b {
 public:
  Vec16() = default;
  Vec16(i32 val) noexcept { reg_ = _mm512_set1_epi32(val); }
  Vec16(const __m512i &reg) noexcept : Vec512b(reg) {}
  Vec16(i32 val1, i32 val2, i32 val3, i32 val4, i32 val5, i32 val6, i32 val7,
        i32 val8, i32 val9, i32 val10, i32 val11, i32 val12, i32 val13,
        i32 val14, i32 val15, i32 val16) noexcept {
    reg_ =
        _mm512_setr_epi32(val1, val2, val3, val4, val5, val6, val7, val8, val9,
                          val10, val11, val12, val13, val14, val15, val16);
  }

  template <typename T>
  void Load(const T *ptr);

  template <typename T>
  void Gather(const T *ptr, const Vec16 &pos);

  ALWAYS_INLINE void Store(i32 *ptr) const {
    _mm512_store_si512(reinterpret_cast<__m512i *>(ptr), reg_);
  }

  ALWAYS_INLINE i32 Extract(u32 index) const {
    alignas(64) i32 x[Size()];
    Store(x);
    return x[index & 15];
  }

  ALWAYS_INLINE i32 operator[](u32 index) const { return Extract(index); }

  ALWAYS_INLINE static constexpr int Size() { return 16; }
};

class Vec8Mask {
 public:
  Vec8Mask() = default;
  Vec8Mask(const __mmask8 &mask) : mask_(mask) {}

  ALWAYS_INLINE u32 ToPositions(u32 *positions, u32 offset) const {
    __m512i sequence = _mm512_setr_epi64(0, 1, 2, 3, 4, 5, 6, 7);
    __m512i pos_vec = _mm512_add_epi64(sequence, _mm512_set1_epi64(offset));
    __m256i pos_vec_comp = _mm512_cvtepi64_epi32(pos_vec);
    _mm256_mask_compressstoreu_epi32(positions, mask_, pos_vec_comp);
    return __builtin_popcountll(mask_);
  }

  ALWAYS_INLINE u32 ToPositions(u32 *positions, const Vec8 &pos) const {
    __m256i pos_comp = _mm512_cvtepi64_epi32(pos);
    _mm256_mask_compressstoreu_epi32(positions, mask_, pos_comp);
    return __builtin_popcountll(mask_);
  }

  ALWAYS_INLINE bool Extract(u32 index) const {
    return (static_cast<u32>(mask_) >> index) & 1;
  }

  ALWAYS_INLINE bool operator[](u32 index) const { return Extract(index); }

  ALWAYS_INLINE operator __mmask8() const { return mask_; }

  ALWAYS_INLINE static constexpr int Size() { return 8; }

 private:
  __mmask8 mask_;
};

class Vec16Mask {
 public:
  Vec16Mask() = default;
  Vec16Mask(const __mmask16 &mask) : mask_(mask) {}

  ALWAYS_INLINE u32 ToPositions(u32 *positions, u32 offset) const {
    __m512i sequence =
        _mm512_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    __m512i pos_vec = _mm512_add_epi32(sequence, _mm512_set1_epi32(offset));
    _mm512_mask_compressstoreu_epi32(positions, mask_, pos_vec);
    return __builtin_popcountll(mask_);
  }

  ALWAYS_INLINE u32 ToPositions(u32 *positions, const Vec16 &pos) const {
    _mm512_mask_compressstoreu_epi32(positions, mask_, pos);
    return __builtin_popcountll(mask_);
  }

  ALWAYS_INLINE bool Extract(u32 index) const {
    return (static_cast<u32>(mask_) >> index) & 1;
  }

  ALWAYS_INLINE bool operator[](u32 index) const { return Extract(index); }

  ALWAYS_INLINE operator __mmask16() const { return mask_; }

  ALWAYS_INLINE static constexpr int Size() { return 16; }

 private:
  __mmask16 mask_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Vec8
///
////////////////////////////////////////////////////////////////////////////////

ALWAYS_INLINE inline Vec512b operator&(const Vec512b &a, const Vec512b &b) {
  return _mm512_and_epi64(a, b);
}

ALWAYS_INLINE inline Vec512b operator|(const Vec512b &a, const Vec512b &b) {
  return _mm512_or_si512(a, b);
}

ALWAYS_INLINE inline Vec512b operator^(const Vec512b &a, const Vec512b &b) {
  return _mm512_xor_si512(a, b);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Vec8
///
////////////////////////////////////////////////////////////////////////////////

template <typename T>
ALWAYS_INLINE inline void Vec8::Load(const T *ptr) {
  using signed_t = std::make_signed_t<T>;
  Load<signed_t>(reinterpret_cast<const signed_t *>(ptr));
}

template <>
ALWAYS_INLINE inline void Vec8::Load<i32>(const i32 *ptr) {
  auto tmp = _mm256_load_si256(reinterpret_cast<const __m256i *>(ptr));
  reg_ = _mm512_cvtepi32_epi64(tmp);
}

template <>
ALWAYS_INLINE inline void Vec8::Load<i64>(const i64 *ptr) {
  reg_ = _mm512_loadu_si512((const __m512i *)ptr);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Vec16
///
////////////////////////////////////////////////////////////////////////////////

template <typename T>
ALWAYS_INLINE inline void Vec16::Load(const T *ptr) {
  using signed_t = std::make_signed_t<T>;
  Load<signed_t>(reinterpret_cast<const signed_t *>(ptr));
}

template <>
ALWAYS_INLINE inline void Vec16::Load<i8>(const i8 *ptr) {
  auto tmp = _mm_loadu_si128(reinterpret_cast<const __m128i *>(ptr));
  reg_ = _mm512_cvtepi8_epi32(tmp);
}

template <>
ALWAYS_INLINE inline void Vec16::Load<i16>(const i16 *ptr) {
  auto tmp = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr));
  reg_ = _mm512_cvtepi16_epi32(tmp);
}

template <>
ALWAYS_INLINE inline void Vec16::Load<i32>(const i32 *ptr) {
  reg_ = _mm512_loadu_si512(ptr);
}

template <typename T>
ALWAYS_INLINE inline void Vec16::Gather(const T *ptr, const Vec16 &pos) {
  using signed_t = std::make_signed_t<T>;
  Gather<signed_t>(reinterpret_cast<const signed_t *>(ptr), pos);
}

template <>
ALWAYS_INLINE inline void Vec16::Gather<i8>(const i8 *ptr, const Vec16 &pos) {
  alignas(64) i32 x[Size()];
  pos.Store(x);
  reg_ = _mm512_setr_epi32(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]],
                           ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]],
                           ptr[x[8]], ptr[x[9]], ptr[x[10]], ptr[x[11]],
                           ptr[x[12]], ptr[x[13]], ptr[x[14]], ptr[x[15]]);
}

template <>
ALWAYS_INLINE inline void Vec16::Gather<i16>(const i16 *ptr, const Vec16 &pos) {
  alignas(64) i32 x[Size()];
  pos.Store(x);
  reg_ = _mm512_setr_epi32(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]],
                           ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]],
                           ptr[x[8]], ptr[x[9]], ptr[x[10]], ptr[x[11]],
                           ptr[x[12]], ptr[x[13]], ptr[x[14]], ptr[x[15]]);
}

template <>
ALWAYS_INLINE inline void Vec16::Gather<i32>(const i32 *ptr, const Vec16 &pos) {
  reg_ = _mm512_i32gather_epi32(pos, ptr, 4);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Vec8 - Comparison Operations
///
////////////////////////////////////////////////////////////////////////////////

ALWAYS_INLINE inline Vec8Mask operator>(const Vec8 &a, const Vec8 &b) noexcept {
  return _mm512_cmpgt_epi64_mask(a, b);
}

ALWAYS_INLINE inline Vec8Mask operator==(const Vec8 &a,
                                         const Vec8 &b) noexcept {
  return _mm512_cmpeq_epi64_mask(a, b);
}

ALWAYS_INLINE inline Vec8Mask operator<(const Vec8 &a, const Vec8 &b) noexcept {
  return _mm512_cmplt_epi64_mask(a, b);
}

ALWAYS_INLINE inline Vec8Mask operator<=(const Vec8 &a,
                                         const Vec8 &b) noexcept {
  return _mm512_cmple_epi64_mask(a, b);
}

ALWAYS_INLINE inline Vec8Mask operator>=(const Vec8 &a,
                                         const Vec8 &b) noexcept {
  return _mm512_cmpge_epi64_mask(a, b);
}

ALWAYS_INLINE inline Vec8Mask operator!=(const Vec8 &a,
                                         const Vec8 &b) noexcept {
  return _mm512_cmpneq_epi64_mask(a, b);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Vec8 - Arithmetic Operations
///
////////////////////////////////////////////////////////////////////////////////

ALWAYS_INLINE inline Vec8 operator+(const Vec8 &a, const Vec8 &b) {
  return _mm512_add_epi64(a, b);
}

ALWAYS_INLINE inline Vec8 operator-(const Vec8 &a, const Vec8 &b) {
  return _mm512_sub_epi64(a, b);
}

ALWAYS_INLINE inline Vec8 &operator+=(Vec8 &a, const Vec8 &b) {
  a = a + b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator&(const Vec8 &a, const Vec8 &b) {
  return _mm512_and_epi64(a, b);
}

ALWAYS_INLINE inline Vec8 operator|(const Vec8 &a, const Vec8 &b) {
  return _mm512_or_epi64(a, b);
}

ALWAYS_INLINE inline Vec8 operator^(const Vec8 &a, const Vec8 &b) {
  return _mm512_xor_epi64(a, b);
}

ALWAYS_INLINE inline Vec8 operator>>(const Vec8 &a, const u32 shift) {
  return _mm512_srli_epi64(a, shift);
}

ALWAYS_INLINE inline Vec8 operator<<(const Vec8 &a, const u32 shift) {
  return _mm512_slli_epi64(a, shift);
}

ALWAYS_INLINE inline Vec8 operator>>(const Vec8 &a, const Vec8 &b) {
  return _mm512_srlv_epi64(a, b);
}

ALWAYS_INLINE inline Vec8 operator<<(const Vec8 &a, const Vec8 &b) {
  return _mm512_sllv_epi64(a, b);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Vec16 - Comparison Operations
///
////////////////////////////////////////////////////////////////////////////////

ALWAYS_INLINE inline Vec16Mask operator>(const Vec16 &a,
                                         const Vec16 &b) noexcept {
  return _mm512_cmpgt_epi32_mask(a, b);
}

ALWAYS_INLINE inline Vec16Mask operator==(const Vec16 &a,
                                          const Vec16 &b) noexcept {
  return _mm512_cmpeq_epi32_mask(a, b);
}

ALWAYS_INLINE inline Vec16Mask operator<(const Vec16 &a,
                                         const Vec16 &b) noexcept {
  return _mm512_cmplt_epi32_mask(a, b);
}

ALWAYS_INLINE inline Vec16Mask operator<=(const Vec16 &a,
                                          const Vec16 &b) noexcept {
  return _mm512_cmple_epi32_mask(a, b);
}

ALWAYS_INLINE inline Vec16Mask operator>=(const Vec16 &a,
                                          const Vec16 &b) noexcept {
  return _mm512_cmpge_epi32_mask(a, b);
}

ALWAYS_INLINE inline Vec16Mask operator!=(const Vec16 &a,
                                          const Vec16 &b) noexcept {
  return _mm512_cmpneq_epi32_mask(a, b);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Vec16 - Arithmetic Operations
///
////////////////////////////////////////////////////////////////////////////////

ALWAYS_INLINE inline Vec16 operator+(const Vec16 &a, const Vec16 &b) {
  return _mm512_add_epi32(a, b);
}

ALWAYS_INLINE inline Vec16 operator-(const Vec16 &a, const Vec16 &b) {
  return _mm512_sub_epi32(a, b);
}

ALWAYS_INLINE inline Vec16 &operator+=(Vec16 &a, const Vec16 &b) {
  a = a + b;
  return a;
}

ALWAYS_INLINE inline Vec16 operator&(const Vec16 &a, const Vec16 &b) {
  return _mm512_and_epi32(a, b);
}

ALWAYS_INLINE inline Vec16 operator|(const Vec16 &a, const Vec16 &b) {
  return _mm512_or_epi32(a, b);
}

ALWAYS_INLINE inline Vec16 operator^(const Vec16 &a, const Vec16 &b) {
  return _mm512_xor_epi32(a, b);
}

ALWAYS_INLINE inline Vec16 operator>>(const Vec16 &a, const u32 shift) {
  return _mm512_srli_epi32(a, shift);
}

ALWAYS_INLINE inline Vec16 operator<<(const Vec16 &a, const u32 shift) {
  return _mm512_slli_epi32(a, shift);
}

ALWAYS_INLINE inline Vec16 operator>>(const Vec16 &a, const Vec16 &b) {
  return _mm512_srlv_epi32(a, b);
}

ALWAYS_INLINE inline Vec16 operator<<(const Vec16 &a, const Vec16 &b) {
  return _mm512_sllv_epi32(a, b);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Filter
///
////////////////////////////////////////////////////////////////////////////////

template <typename T, typename Enable = void>
struct FilterVecSizer;

template <>
struct FilterVecSizer<i8> {
  using Vec = Vec16;
  using VecMask = Vec16Mask;
};

template <>
struct FilterVecSizer<i16> {
  using Vec = Vec16;
  using VecMask = Vec16Mask;
};

template <>
struct FilterVecSizer<i32> {
  using Vec = Vec16;
  using VecMask = Vec16Mask;
};

template <>
struct FilterVecSizer<i64> {
  using Vec = Vec8;
  using VecMask = Vec8Mask;
};

template <typename T>
struct FilterVecSizer<T, std::enable_if_t<std::is_unsigned_v<T>>>
    : public FilterVecSizer<std::make_signed_t<T>> {};

template <typename T, template <typename> typename Compare>
static inline u32 FilterVectorByVal(const T *RESTRICT in, u32 in_count, T val,
                                    u32 *RESTRICT out, const u32 *RESTRICT sel,
                                    u32 &RESTRICT in_pos) {
  using Vec = typename FilterVecSizer<T>::Vec;
  using VecMask = typename FilterVecSizer<T>::VecMask;

  const Compare cmp;

  const Vec xval(val);

  u32 out_pos = 0;

  if (sel == nullptr) {
    Vec in_vec;
    for (in_pos = 0; in_pos + Vec::Size() < in_count; in_pos += Vec::Size()) {
      in_vec.Load(in + in_pos);
      VecMask mask = cmp(in_vec, xval);
      out_pos += mask.ToPositions(out + out_pos, in_pos);
    }
  } else {
    Vec in_vec, sel_vec;
    for (in_pos = 0; in_pos + Vec::Size() < in_count; in_pos += Vec::Size()) {
      sel_vec.Load(sel + in_pos);
      in_vec.Gather(in, sel_vec);
      VecMask mask = cmp(in_vec, xval);
      out_pos += mask.ToPositions(out + out_pos, sel_vec);
    }
  }

  return out_pos;
}

}  // namespace tpl::util::simd
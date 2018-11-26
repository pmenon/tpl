#pragma once

#include <immintrin.h>

#include "util/common.h"
#include "util/macros.h"

namespace tpl::util::simd {

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
  void Load(const T *ptr) {
    reg_ = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(ptr));
  }

  ALWAYS_INLINE void Store(i64 *ptr) const {
    _mm512_store_si512(reinterpret_cast<__m512i *>(ptr), reg());
  }

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
    _mm512_mask_compressstoreu_epi64(reinterpret_cast<__m512i *>(positions),
                                     mask_, pos_vec);
    return __builtin_popcountll(mask_);
  }

  ALWAYS_INLINE bool Extract(u32 index) const {
    return (static_cast<u32>(mask_) >> index) & 1;
  }

  ALWAYS_INLINE bool operator[](uint32_t index) const { return Extract(index); }

  ALWAYS_INLINE operator __mmask8() const { return mask_; }

  static constexpr int Size() { return 8; }

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
    _mm512_mask_compressstoreu_epi32(reinterpret_cast<__m512i *>(positions),
                                     mask_, pos_vec);
    return __builtin_popcountll(mask_);
  }

  ALWAYS_INLINE bool Extract(u32 index) const {
    return (static_cast<u32>(mask_) >> index) & 1;
  }

  ALWAYS_INLINE bool operator[](uint32_t index) const { return Extract(index); }

  ALWAYS_INLINE operator __mmask16() const { return mask_; }

  static constexpr int Size() { return 16; }

 private:
  __mmask16 mask_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Vec16
///
////////////////////////////////////////////////////////////////////////////////

template <>
ALWAYS_INLINE inline void Vec16::Load<i8>(const i8 *ptr) {
  auto tmp = _mm_loadu_si128((const __m128i *)ptr);
  reg_ = _mm512_cvtepi8_epi32(tmp);
}

template <>
ALWAYS_INLINE inline void Vec16::Load<u8>(const u8 *ptr) {
  return Load<i8>(reinterpret_cast<const i8 *>(ptr));
}

template <>
ALWAYS_INLINE inline void Vec16::Load<i16>(const i16 *ptr) {
  auto tmp = _mm256_loadu_si256((const __m256i *)ptr);
  reg_ = _mm512_cvtepi16_epi32(tmp);
}

template <>
ALWAYS_INLINE inline void Vec16::Load<u16>(const u16 *ptr) {
  return Load<i16>(reinterpret_cast<const i16 *>(ptr));
}

template <>
ALWAYS_INLINE inline void Vec16::Load<i32>(const i32 *ptr) {
  reg_ = _mm512_loadu_si512((const __m512i *)ptr);
}

template <>
ALWAYS_INLINE inline void Vec16::Load<u32>(const u32 *ptr) {
  return Load<i32>(reinterpret_cast<const i32 *>(ptr));
}

////////////////////////////////////////////////////////////////////////////////
///
/// Vec16 - Comparison Operations
///
////////////////////////////////////////////////////////////////////////////////

static ALWAYS_INLINE inline Vec16Mask operator>(const Vec16 &lhs,
                                                const Vec16 &rhs) noexcept {
  return _mm512_cmpgt_epi32_mask(lhs, rhs);
}

static ALWAYS_INLINE inline Vec16Mask operator==(const Vec16 &lhs,
                                                 const Vec16 &rhs) noexcept {
  return _mm512_cmpeq_epi32_mask(lhs, rhs);
}

static ALWAYS_INLINE inline Vec16Mask operator<(const Vec16 &lhs,
                                                const Vec16 &rhs) noexcept {
  return _mm512_cmplt_epi32_mask(lhs, rhs);
}

static ALWAYS_INLINE inline Vec16Mask operator<=(const Vec16 &lhs,
                                                 const Vec16 &rhs) noexcept {
  return _mm512_cmple_epi32_mask(lhs, rhs);
}

static ALWAYS_INLINE inline Vec16Mask operator>=(const Vec16 &lhs,
                                                 const Vec16 &rhs) noexcept {
  return _mm512_cmpge_epi32_mask(lhs, rhs);
}

static ALWAYS_INLINE inline Vec16Mask operator!=(const Vec16 &lhs,
                                                 const Vec16 &rhs) noexcept {
  return _mm512_cmpneq_epi32_mask(lhs, rhs);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Vec16 - Arithmetic Operations
///
////////////////////////////////////////////////////////////////////////////////

static ALWAYS_INLINE inline Vec16 operator+(const Vec16 &lhs,
                                            const Vec16 &rhs) {
  return _mm512_add_epi32(lhs, rhs);
}

static ALWAYS_INLINE inline Vec16 operator-(const Vec16 &lhs,
                                            const Vec16 &rhs) {
  return _mm512_sub_epi32(lhs, rhs);
}

static ALWAYS_INLINE inline Vec16 &operator+=(Vec16 &lhs, const Vec16 &rhs) {
  lhs = lhs + rhs;
  return lhs;
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

template <typename T, typename Compare>
static inline u32 Filter(const T *RESTRICT in, u32 in_count, T val,
                         u32 *RESTRICT out, u32 &in_pos) noexcept {
  using Vec = typename FilterVecSizer<T>::Vec;
  using VecMask = typename FilterVecSizer<T>::VecMask;

  Compare op{};

  u32 out_pos = 0;

  Vec xval(val);
  Vec in_vec;

  for (in_pos = 0; in_pos + Vec::Size() < in_count; in_pos += Vec::Size()) {
    in_vec.Load(in + in_pos);
    VecMask mask = op(in_vec, xval);
    out_pos += mask.ToPositions(out + out_pos, in_pos);
  }
  return out_pos;
}

}  // namespace tpl::util::simd
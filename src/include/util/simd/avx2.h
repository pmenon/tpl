#pragma once

#include <immintrin.h>

#include "util/common.h"
#include "util/macros.h"
#include "util/simd/types.h"

#ifndef SIMD_TOP_LEVEL
#error \
    "Don't include <util/simd/avx2.h> directly; instead, include <util/simd.h>"
#endif

namespace tpl::util::simd {

#define USE_GATHER 0

/// A 256-bit SIMD register vector. This is a purely internal class that holds
/// common functions for other user-visible vector classes
class Vec256b {
 public:
  Vec256b() = default;
  Vec256b(const __m256i &reg) : reg_(reg) {}

  // Type-cast operator so that Vec*'s can be used directly with intrinsics
  ALWAYS_INLINE operator __m256i() const { return reg_; }

 protected:
  const __m256i &reg() const { return reg_; }

 protected:
  __m256i reg_;
};

/// A 256-bit SIMD register interpreted as 4 64-bit integer values
class Vec4 : public Vec256b {
  friend class Vec4Mask;

 public:
  Vec4() = default;
  Vec4(i64 val) noexcept { reg_ = _mm256_set1_epi64x(val); }
  Vec4(const __m256i &reg) noexcept : Vec256b(reg) {}
  Vec4(i64 val1, i64 val2, i64 val3, i64 val4) noexcept {
    reg_ = _mm256_setr_epi64x(val1, val2, val3, val4);
  }

  /// Load 4 64-bit values stored contiguously from the input pointer \p ptr.
  /// The underlying data type of the array \p ptr can be either 32-bit integers
  /// in which case the 4 values will be loaded and upcasted, or 64-bit integers
  /// which is the trivial case
  /// \tparam T The underlying data type of the pointer
  /// \param ptr The input pointer to read from
  template <typename T>
  void Load(const T *ptr);

  /// Gather non-contiguous elements from the input array \p ptr stored at
  /// index positions from \t pos
  /// \tparam T The data type of the underlying array
  /// \param ptr The input array
  /// \param pos The list of positions in the input array to gather
  template <typename T>
  ALWAYS_INLINE inline void Gather(const T *ptr, const Vec4 &pos) {
#if USE_GATHER == 1
    reg_ = _mm256_i64gather_epi64(ptr, pos, 8);
#else
    alignas(32) i64 x[Size()];
    pos.Store(x);
    reg_ = _mm256_setr_epi64x(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]]);
#endif
  }

  /// Store the contents of this vector contiguously into the input array \p ptr
  /// \param ptr The array to write into
  ALWAYS_INLINE inline void Store(i64 *ptr) const {
    _mm256_store_si256(reinterpret_cast<__m256i *>(ptr), reg());
  }

  /// Return the number of elements that can be stored in this vector
  static constexpr u32 Size() { return 4; }

  // Extract the integer at the given index from this vector
  ALWAYS_INLINE inline i64 Extract(u32 index) const {
    alignas(32) i64 x[Size()];
    Store(x);
    return x[index & 3];
  }

  // Extract the integer at the given index from this vector
  ALWAYS_INLINE inline i64 operator[](u32 index) const {
    return Extract(index);
  }
};

/// A 256-bit SIMD register interpreted as 8 32-bit integer values
class Vec8 : public Vec256b {
 public:
  Vec8() = default;
  Vec8(i32 val) noexcept { reg_ = _mm256_set1_epi32(val); }
  Vec8(const __m256i &reg) noexcept : Vec256b(reg) {}
  Vec8(i32 val1, i32 val2, i32 val3, i32 val4, i32 val5, i32 val6, i32 val7,
       i32 val8) noexcept {
    reg_ = _mm256_setr_epi32(val1, val2, val3, val4, val5, val6, val7, val8);
  }

  /// Load 8 32-bit values stored contiguously from the input pointer \p ptr.
  /// The underlying data type of the array \p ptr can be either 8-bit, 16-bit,
  /// or 32-bit integers. Up-casting is performed when appropriate.
  /// \tparam T The underlying data type of the pointer
  /// \param ptr The input pointer to read from
  template <typename T>
  void Load(const T *ptr);

  /// Gather non-contiguous elements from the input array \p ptr stored at
  /// index positions from \t pos
  /// \tparam T The data type of the underlying array
  /// \param ptr The input array
  /// \param pos The list of positions in the input array to gather
  template <typename T>
  void Gather(const T *ptr, const Vec8 &pos);

  /// Store the contents of this vector contiguously into the input array \p ptr
  /// \param ptr The array to write into
  ALWAYS_INLINE inline void Store(i32 *ptr) const {
    _mm256_store_si256(reinterpret_cast<__m256i *>(ptr), reg());
  }

  /// Return the number of elements that can be stored in this vector
  static constexpr u32 Size() { return 8; }

  ALWAYS_INLINE inline i32 Extract(u32 index) const {
    TPL_ASSERT(index < 8, "Out-of-bounds mask element access");
    alignas(32) i32 x[Size()];
    Store(x);
    return x[index & 7];
  }

  ALWAYS_INLINE inline i32 operator[](u32 index) const {
    return Extract(index);
  }
};

class Vec8Mask : public Vec8 {
 public:
  Vec8Mask() = default;
  Vec8Mask(const __m256i &reg) : Vec8(reg) {}

  ALWAYS_INLINE inline i32 Extract(u32 index) const {
    return Vec8::Extract(index) != 0;
  }

  ALWAYS_INLINE inline i32 operator[](u32 index) const {
    return Extract(index);
  }

  ALWAYS_INLINE inline u32 ToPositions(u32 *positions, u32 offset) const {
    i32 mask = _mm256_movemask_ps(_mm256_castsi256_ps(reg()));
    TPL_ASSERT(mask < 256, "8-bit mask must be less than 256");
    __m128i match_pos_scaled = _mm_loadl_epi64(
        reinterpret_cast<const __m128i *>(&k8BitMatchLUT[mask]));
    __m256i match_pos = _mm256_cvtepi8_epi32(match_pos_scaled);
    __m256i pos_vec = _mm256_add_epi32(_mm256_set1_epi32(offset), match_pos);
    _mm256_storeu_si256(reinterpret_cast<__m256i *>(positions), pos_vec);
    return __builtin_popcount(mask);
  }

  ALWAYS_INLINE inline u32 ToPositions(u32 *positions, const Vec8 &pos) const {
    i32 mask = _mm256_movemask_ps(_mm256_castsi256_ps(reg()));
    TPL_ASSERT(mask < 256, "8-bit mask must be less than 256");
    __m128i perm_comp = _mm_loadl_epi64(
        reinterpret_cast<const __m128i *>(&k8BitMatchLUT[mask]));
    __m256i perm = _mm256_cvtepi8_epi32(perm_comp);
    __m256i perm_pos = _mm256_permutevar8x32_epi32(pos, perm);
    __m256i perm_mask = _mm256_permutevar8x32_epi32(reg(), perm);
    _mm256_maskstore_epi32(reinterpret_cast<i32 *>(positions), perm_mask,
                           perm_pos);
    return __builtin_popcount(mask);
  }
};

class Vec4Mask : public Vec4 {
 public:
  Vec4Mask() = default;
  Vec4Mask(const __m256i &reg) : Vec4(reg) {}

  ALWAYS_INLINE inline i32 Extract(u32 index) const {
    TPL_ASSERT(index < 4, "Out-of-bounds mask element access");
    return Vec4::Extract(index) != 0;
  }

  ALWAYS_INLINE inline i32 operator[](u32 index) const {
    TPL_ASSERT(index < 4, "Out-of-bounds mask element access");
    return Extract(index);
  }

  ALWAYS_INLINE inline u32 ToPositions(u32 *positions, u32 offset) const {
    i32 mask = _mm256_movemask_pd(_mm256_castsi256_pd(reg()));
    TPL_ASSERT(mask < 16, "4-bit mask must be less than 16");
    __m128i match_pos_scaled = _mm_loadl_epi64((__m128i *)&k4BitMatchLUT[mask]);
    __m128i match_pos = _mm_cvtepi16_epi32(match_pos_scaled);
    __m128i pos_vec = _mm_add_epi32(_mm_set1_epi32(offset), match_pos);
    _mm_storeu_si128(reinterpret_cast<__m128i *>(positions), pos_vec);
    return __builtin_popcount(mask);
  }

  ALWAYS_INLINE inline u32 ToPositions(u32 *positions, const Vec4 &pos) const {
    i32 mask = _mm256_movemask_pd(_mm256_castsi256_pd(reg()));
    TPL_ASSERT(mask < 16, "4-bit mask must be less than 16");

    // TODO(pmenon): Fix this slowness!
    {
      alignas(32) i64 m_arr[Size()];
      alignas(32) i64 p_arr[Size()];
      Store(m_arr);
      pos.Store(p_arr);
      for (u32 idx = 0, i = 0; i < 4; i++) {
        positions[idx] = static_cast<u32>(p_arr[i]);
        idx += (m_arr[i] != 0);
      }
    }

    return __builtin_popcount(mask);
  }
};

// ---------------------------------------------------------
// Vec256b - Generic Bitwise Operations
// ---------------------------------------------------------

// Bit-wise NOT
ALWAYS_INLINE inline Vec256b operator~(const Vec256b &v) noexcept {
  return _mm256_xor_si256(v, _mm256_set1_epi32(-1));
}

ALWAYS_INLINE inline Vec256b operator&(const Vec256b &a,
                                       const Vec256b &b) noexcept {
  return _mm256_and_si256(a, b);
}

ALWAYS_INLINE inline Vec256b operator|(const Vec256b &a,
                                       const Vec256b &b) noexcept {
  return _mm256_or_si256(a, b);
}

ALWAYS_INLINE inline Vec256b operator^(const Vec256b &a,
                                       const Vec256b &b) noexcept {
  return _mm256_xor_si256(a, b);
}

// ---------------------------------------------------------
// Vec8Mask
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec8Mask operator&(const Vec8Mask &a,
                                        const Vec8Mask &b) noexcept {
  return Vec8Mask(Vec256b(a) & Vec256b(b));
}

// ---------------------------------------------------------
// Vec4Mask
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec4Mask operator&(const Vec4Mask &a,
                                        const Vec4Mask &b) noexcept {
  return Vec4Mask(Vec256b(a) & Vec256b(b));
}

// ---------------------------------------------------------
// Vec4
// ---------------------------------------------------------

template <typename T>
ALWAYS_INLINE inline void Vec4::Load(const T *ptr) {
  using signed_t = std::make_signed_t<T>;
  Load<signed_t>(reinterpret_cast<const signed_t *>(ptr));
}

template <>
ALWAYS_INLINE inline void Vec4::Load<i32>(const i32 *ptr) {
  auto tmp = _mm_loadu_si128((const __m128i *)ptr);
  reg_ = _mm256_cvtepi32_epi64(tmp);
}

template <>
ALWAYS_INLINE inline void Vec4::Load<i64>(const i64 *ptr) {
  reg_ = _mm256_loadu_si256((const __m256i *)ptr);
}

// ---------------------------------------------------------
// Vec4 - Comparison Operations
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec4Mask operator>(const Vec4 &a, const Vec4 &b) noexcept {
  return _mm256_cmpgt_epi64(a, b);
}

ALWAYS_INLINE inline Vec4Mask operator==(const Vec4 &a,
                                         const Vec4 &b) noexcept {
  return _mm256_cmpeq_epi64(a, b);
}

ALWAYS_INLINE inline Vec4Mask operator>=(const Vec4 &a,
                                         const Vec4 &b) noexcept {
  return Vec4Mask(~(b > a));
}

ALWAYS_INLINE inline Vec4Mask operator<(const Vec4 &a, const Vec4 &b) noexcept {
  return b > a;
}

ALWAYS_INLINE inline Vec4Mask operator<=(const Vec4 &a,
                                         const Vec4 &b) noexcept {
  return b >= a;
}

ALWAYS_INLINE inline Vec4Mask operator!=(const Vec4 &a,
                                         const Vec4 &b) noexcept {
  return Vec4Mask(~Vec256b(a == b));
}

// ---------------------------------------------------------
// Vec4 - Arithmetic Operations
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec4 operator+(const Vec4 &a, const Vec4 &b) {
  return _mm256_add_epi64(a, b);
}

ALWAYS_INLINE inline Vec4 &operator+=(Vec4 &a, const Vec4 &b) {
  a = a + b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator-(const Vec4 &a, const Vec4 &b) {
  return _mm256_sub_epi64(a, b);
}

ALWAYS_INLINE inline Vec4 &operator-=(Vec4 &a, const Vec4 &b) {
  a = a - b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator&(const Vec4 &a, const Vec4 &b) {
  return Vec4(Vec256b(a) & Vec256b(b));
}

ALWAYS_INLINE inline Vec4 &operator&=(Vec4 &a, const Vec4 &b) {
  a = a & b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator|(const Vec4 &a, const Vec4 &b) {
  return Vec4(Vec256b(a) | Vec256b(b));
}

ALWAYS_INLINE inline Vec4 &operator|=(Vec4 &a, const Vec4 &b) {
  a = a | b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator^(const Vec4 &a, const Vec4 &b) {
  return Vec4(Vec256b(a) ^ Vec256b(b));
}

ALWAYS_INLINE inline Vec4 &operator^=(Vec4 &a, const Vec4 &b) {
  a = a ^ b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator>>(const Vec4 &a, const u32 shift) {
  return _mm256_srli_epi64(a, shift);
}

ALWAYS_INLINE inline Vec4 &operator>>=(Vec4 &a, const u32 shift) {
  a = a >> shift;
  return a;
}

ALWAYS_INLINE inline Vec4 operator<<(const Vec4 &a, const u32 shift) {
  return _mm256_slli_epi64(a, shift);
}

ALWAYS_INLINE inline Vec4 &operator<<=(Vec4 &a, const u32 shift) {
  a = a << shift;
  return a;
}

ALWAYS_INLINE inline Vec4 operator>>(const Vec4 &a, const Vec4 &b) {
  return _mm256_srlv_epi64(a, b);
}

ALWAYS_INLINE inline Vec4 &operator>>=(Vec4 &a, const Vec4 &b) {
  a = a >> b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator<<(const Vec4 &a, const Vec4 &b) {
  return _mm256_sllv_epi64(a, b);
}

ALWAYS_INLINE inline Vec4 &operator<<=(Vec4 &a, const Vec4 &b) {
  a = a << b;
  return a;
}

// ---------------------------------------------------------
// Vec8
// ---------------------------------------------------------

template <typename T>
ALWAYS_INLINE inline void Vec8::Load(const T *ptr) {
  using signed_t = std::make_signed_t<T>;
  Load<signed_t>(reinterpret_cast<const signed_t *>(ptr));
};

template <>
ALWAYS_INLINE inline void Vec8::Load<i8>(const i8 *ptr) {
  auto tmp = _mm_loadu_si128((const __m128i *)ptr);
  reg_ = _mm256_cvtepi8_epi32(tmp);
}

template <>
ALWAYS_INLINE inline void Vec8::Load<i16>(const i16 *ptr) {
  auto tmp = _mm_loadu_si128((const __m128i *)ptr);
  reg_ = _mm256_cvtepi16_epi32(tmp);
}

template <>
ALWAYS_INLINE inline void Vec8::Load<i32>(const i32 *ptr) {
  reg_ = _mm256_loadu_si256((const __m256i *)ptr);
}

template <typename T>
ALWAYS_INLINE inline void Vec8::Gather(const T *ptr, const Vec8 &pos) {
  using signed_t = std::make_signed_t<T>;
  Gather<signed_t>(reinterpret_cast<const signed_t *>(ptr), pos);
}

template <>
NEVER_INLINE inline void Vec8::Gather<i8>(const i8 *ptr, const Vec8 &pos) {
#if USE_GATHER == 1
  reg_ = _mm256_i32gather_epi32(ptr, pos, 1);
  reg_ = _mm256_srai_epi32(reg_, 24);
#else
  alignas(32) i32 x[Size()];
  pos.Store(x);
  reg_ = _mm256_setr_epi32(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]],
                           ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]]);
#endif
}

template <>
ALWAYS_INLINE inline void Vec8::Gather<i16>(const i16 *ptr, const Vec8 &pos) {
#if USE_GATHER == 1
  reg_ = _mm256_i32gather_epi32(ptr, pos, 2);
  reg_ = _mm256_srai_epi32(reg_, 16);
#else
  alignas(32) i32 x[Size()];
  pos.Store(x);
  reg_ = _mm256_setr_epi32(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]],
                           ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]]);
#endif
}

template <>
ALWAYS_INLINE inline void Vec8::Gather<i32>(const i32 *ptr, const Vec8 &pos) {
#if USE_GATHER == 1
  reg_ = _mm256_i32gather_epi32(ptr, pos, 4);
#else
  alignas(32) i32 x[Size()];
  pos.Store(x);
  reg_ = _mm256_setr_epi32(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]],
                           ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]]);
#endif
}

// ---------------------------------------------------------
// Vec8 - Comparison Operations
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec8Mask operator>(const Vec8 &a, const Vec8 &b) noexcept {
  return _mm256_cmpgt_epi32(a, b);
}

ALWAYS_INLINE inline Vec8Mask operator==(const Vec8 &a,
                                         const Vec8 &b) noexcept {
  return _mm256_cmpeq_epi32(a, b);
}

ALWAYS_INLINE inline Vec8Mask operator>=(const Vec8 &a,
                                         const Vec8 &b) noexcept {
  __m256i max_a_b = _mm256_max_epu32(a, b);
  return _mm256_cmpeq_epi32(a, max_a_b);
}

ALWAYS_INLINE inline Vec8Mask operator<(const Vec8 &a, const Vec8 &b) noexcept {
  return b > a;
}

ALWAYS_INLINE inline Vec8Mask operator<=(const Vec8 &a,
                                         const Vec8 &b) noexcept {
  return b >= a;
}

ALWAYS_INLINE inline Vec8Mask operator!=(const Vec8 &a,
                                         const Vec8 &b) noexcept {
  return Vec8Mask(~Vec256b(a == b));
}

// ---------------------------------------------------------
// Vec8 - Arithmetic Operations
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec8 operator+(const Vec8 &a, const Vec8 &b) {
  return _mm256_add_epi32(a, b);
}

ALWAYS_INLINE inline Vec8 &operator+=(Vec8 &a, const Vec8 &b) {
  a = a + b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator-(const Vec8 &a, const Vec8 &b) {
  return _mm256_sub_epi32(a, b);
}

ALWAYS_INLINE inline Vec8 &operator-=(Vec8 &a, const Vec8 &b) {
  a = a - b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator*(const Vec8 &a, const Vec8 &b) {
  return _mm256_mullo_epi32(a, b);
}

ALWAYS_INLINE inline Vec8 &operator*=(Vec8 &a, const Vec8 &b) {
  a = a * b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator&(const Vec8 &a, const Vec8 &b) {
  return Vec8(Vec256b(a) & Vec256b(b));
}

ALWAYS_INLINE inline Vec8 &operator&=(Vec8 &a, const Vec8 &b) {
  a = a & b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator|(const Vec8 &a, const Vec8 &b) {
  return Vec8(Vec256b(a) | Vec256b(b));
}

ALWAYS_INLINE inline Vec8 &operator|=(Vec8 &a, const Vec8 &b) {
  a = a | b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator^(const Vec8 &a, const Vec8 &b) {
  return Vec8(Vec256b(a) ^ Vec256b(b));
}

ALWAYS_INLINE inline Vec8 &operator^=(Vec8 &a, const Vec8 &b) {
  a = a ^ b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator>>(const Vec8 &a, const u32 shift) {
  return _mm256_srli_epi32(a, shift);
}

ALWAYS_INLINE inline Vec8 &operator>>=(Vec8 &a, const u32 shift) {
  a = a >> shift;
  return a;
}

ALWAYS_INLINE inline Vec8 operator<<(const Vec8 &a, const u32 shift) {
  return _mm256_slli_epi32(a, shift);
}

ALWAYS_INLINE inline Vec8 &operator<<=(Vec8 &a, const u32 shift) {
  a = a << shift;
  return a;
}

ALWAYS_INLINE inline Vec8 operator>>(const Vec8 &a, const Vec8 &b) {
  return _mm256_srlv_epi32(a, b);
}

ALWAYS_INLINE inline Vec8 &operator>>=(Vec8 &a, const Vec8 &b) {
  a = a >> b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator<<(const Vec8 &a, const Vec8 &b) {
  return _mm256_sllv_epi32(a, b);
}

ALWAYS_INLINE inline Vec8 &operator<<=(Vec8 &a, const Vec8 &b) {
  a = a << b;
  return a;
}

// ---------------------------------------------------------
// Filter
// ---------------------------------------------------------

template <typename T, typename Enable = void>
struct FilterVecSizer;

template <>
struct FilterVecSizer<i8> {
  using Vec = Vec8;
  using VecMask = Vec8Mask;
};

template <>
struct FilterVecSizer<i16> {
  using Vec = Vec8;
  using VecMask = Vec8Mask;
};

template <>
struct FilterVecSizer<i32> {
  using Vec = Vec8;
  using VecMask = Vec8Mask;
};

template <>
struct FilterVecSizer<i64> {
  using Vec = Vec4;
  using VecMask = Vec4Mask;
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
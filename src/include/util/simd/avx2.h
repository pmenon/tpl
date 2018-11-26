#pragma once

#include <immintrin.h>

#include "util/common.h"
#include "util/macros.h"

#ifndef SIMD_TOP_LEVEL
#error \
    "Don't include <util/simd/avx2.h> directly; instead, include <util/simd.h>"
#endif

namespace tpl::util::simd {

alignas(32) static constexpr const u64 k8BitMatchLUT[256] = {
    0x0001020304050607ull, 0x0102030405060700ull, 0x0002030405060701ull,
    0x0203040506070100ull, 0x0001030405060702ull, 0x0103040506070200ull,
    0x0003040506070201ull, 0x0304050607020100ull, 0x0001020405060703ull,
    0x0102040506070300ull, 0x0002040506070301ull, 0x0204050607030100ull,
    0x0001040506070302ull, 0x0104050607030200ull, 0x0004050607030201ull,
    0x0405060703020100ull, 0x0001020305060704ull, 0x0102030506070400ull,
    0x0002030506070401ull, 0x0203050607040100ull, 0x0001030506070402ull,
    0x0103050607040200ull, 0x0003050607040201ull, 0x0305060704020100ull,
    0x0001020506070403ull, 0x0102050607040300ull, 0x0002050607040301ull,
    0x0205060704030100ull, 0x0001050607040302ull, 0x0105060704030200ull,
    0x0005060704030201ull, 0x0506070403020100ull, 0x0001020304060705ull,
    0x0102030406070500ull, 0x0002030406070501ull, 0x0203040607050100ull,
    0x0001030406070502ull, 0x0103040607050200ull, 0x0003040607050201ull,
    0x0304060705020100ull, 0x0001020406070503ull, 0x0102040607050300ull,
    0x0002040607050301ull, 0x0204060705030100ull, 0x0001040607050302ull,
    0x0104060705030200ull, 0x0004060705030201ull, 0x0406070503020100ull,
    0x0001020306070504ull, 0x0102030607050400ull, 0x0002030607050401ull,
    0x0203060705040100ull, 0x0001030607050402ull, 0x0103060705040200ull,
    0x0003060705040201ull, 0x0306070504020100ull, 0x0001020607050403ull,
    0x0102060705040300ull, 0x0002060705040301ull, 0x0206070504030100ull,
    0x0001060705040302ull, 0x0106070504030200ull, 0x0006070504030201ull,
    0x0607050403020100ull, 0x0001020304050706ull, 0x0102030405070600ull,
    0x0002030405070601ull, 0x0203040507060100ull, 0x0001030405070602ull,
    0x0103040507060200ull, 0x0003040507060201ull, 0x0304050706020100ull,
    0x0001020405070603ull, 0x0102040507060300ull, 0x0002040507060301ull,
    0x0204050706030100ull, 0x0001040507060302ull, 0x0104050706030200ull,
    0x0004050706030201ull, 0x0405070603020100ull, 0x0001020305070604ull,
    0x0102030507060400ull, 0x0002030507060401ull, 0x0203050706040100ull,
    0x0001030507060402ull, 0x0103050706040200ull, 0x0003050706040201ull,
    0x0305070604020100ull, 0x0001020507060403ull, 0x0102050706040300ull,
    0x0002050706040301ull, 0x0205070604030100ull, 0x0001050706040302ull,
    0x0105070604030200ull, 0x0005070604030201ull, 0x0507060403020100ull,
    0x0001020304070605ull, 0x0102030407060500ull, 0x0002030407060501ull,
    0x0203040706050100ull, 0x0001030407060502ull, 0x0103040706050200ull,
    0x0003040706050201ull, 0x0304070605020100ull, 0x0001020407060503ull,
    0x0102040706050300ull, 0x0002040706050301ull, 0x0204070605030100ull,
    0x0001040706050302ull, 0x0104070605030200ull, 0x0004070605030201ull,
    0x0407060503020100ull, 0x0001020307060504ull, 0x0102030706050400ull,
    0x0002030706050401ull, 0x0203070605040100ull, 0x0001030706050402ull,
    0x0103070605040200ull, 0x0003070605040201ull, 0x0307060504020100ull,
    0x0001020706050403ull, 0x0102070605040300ull, 0x0002070605040301ull,
    0x0207060504030100ull, 0x0001070605040302ull, 0x0107060504030200ull,
    0x0007060504030201ull, 0x0706050403020100ull, 0x0001020304050607ull,
    0x0102030405060700ull, 0x0002030405060701ull, 0x0203040506070100ull,
    0x0001030405060702ull, 0x0103040506070200ull, 0x0003040506070201ull,
    0x0304050607020100ull, 0x0001020405060703ull, 0x0102040506070300ull,
    0x0002040506070301ull, 0x0204050607030100ull, 0x0001040506070302ull,
    0x0104050607030200ull, 0x0004050607030201ull, 0x0405060703020100ull,
    0x0001020305060704ull, 0x0102030506070400ull, 0x0002030506070401ull,
    0x0203050607040100ull, 0x0001030506070402ull, 0x0103050607040200ull,
    0x0003050607040201ull, 0x0305060704020100ull, 0x0001020506070403ull,
    0x0102050607040300ull, 0x0002050607040301ull, 0x0205060704030100ull,
    0x0001050607040302ull, 0x0105060704030200ull, 0x0005060704030201ull,
    0x0506070403020100ull, 0x0001020304060705ull, 0x0102030406070500ull,
    0x0002030406070501ull, 0x0203040607050100ull, 0x0001030406070502ull,
    0x0103040607050200ull, 0x0003040607050201ull, 0x0304060705020100ull,
    0x0001020406070503ull, 0x0102040607050300ull, 0x0002040607050301ull,
    0x0204060705030100ull, 0x0001040607050302ull, 0x0104060705030200ull,
    0x0004060705030201ull, 0x0406070503020100ull, 0x0001020306070504ull,
    0x0102030607050400ull, 0x0002030607050401ull, 0x0203060705040100ull,
    0x0001030607050402ull, 0x0103060705040200ull, 0x0003060705040201ull,
    0x0306070504020100ull, 0x0001020607050403ull, 0x0102060705040300ull,
    0x0002060705040301ull, 0x0206070504030100ull, 0x0001060705040302ull,
    0x0106070504030200ull, 0x0006070504030201ull, 0x0607050403020100ull,
    0x0001020304050706ull, 0x0102030405070600ull, 0x0002030405070601ull,
    0x0203040507060100ull, 0x0001030405070602ull, 0x0103040507060200ull,
    0x0003040507060201ull, 0x0304050706020100ull, 0x0001020405070603ull,
    0x0102040507060300ull, 0x0002040507060301ull, 0x0204050706030100ull,
    0x0001040507060302ull, 0x0104050706030200ull, 0x0004050706030201ull,
    0x0405070603020100ull, 0x0001020305070604ull, 0x0102030507060400ull,
    0x0002030507060401ull, 0x0203050706040100ull, 0x0001030507060402ull,
    0x0103050706040200ull, 0x0003050706040201ull, 0x0305070604020100ull,
    0x0001020507060403ull, 0x0102050706040300ull, 0x0002050706040301ull,
    0x0205070604030100ull, 0x0001050706040302ull, 0x0105070604030200ull,
    0x0005070604030201ull, 0x0507060403020100ull, 0x0001020304070605ull,
    0x0102030407060500ull, 0x0002030407060501ull, 0x0203040706050100ull,
    0x0001030407060502ull, 0x0103040706050200ull, 0x0003040706050201ull,
    0x0304070605020100ull, 0x0001020407060503ull, 0x0102040706050300ull,
    0x0002040706050301ull, 0x0204070605030100ull, 0x0001040706050302ull,
    0x0104070605030200ull, 0x0004070605030201ull, 0x0407060503020100ull,
    0x0001020307060504ull, 0x0102030706050400ull, 0x0002030706050401ull,
    0x0203070605040100ull, 0x0001030706050402ull, 0x0103070605040200ull,
    0x0003070605040201ull, 0x0307060504020100ull, 0x0001020706050403ull,
    0x0102070605040300ull, 0x0002070605040301ull, 0x0207060504030100ull,
    0x0001070605040302ull, 0x0107060504030200ull, 0x0007060504030201ull,
    0x0706050403020100ull};

alignas(32) static constexpr const u64 k4BitMatchLUT[16] = {
    0x0000000100020003ull, 0x0001000200030000ull, 0x0000000200030001ull,
    0x0002000300010000ull, 0x0000000100030002ull, 0x0001000300020000ull,
    0x0000000300020001ull, 0x0003000200010000ull, 0x0000000100020003ull,
    0x0001000200030000ull, 0x0000000200030001ull, 0x0002000300010000ull,
    0x0000000100030002ull, 0x0001000300020000ull, 0x0000000300020001ull,
    0x0003000200010000ull};

/**
 * A 256-bit SIMD register vector. This is a purely internal class that holds
 * common functions for other user-visible vector classes
 */
class Vec512b {
 public:
  Vec512b() = default;
  Vec512b(const __m256i &reg) : reg_(reg) {}

  // Type-cast operator so that Vec*'s can be used directly with intrinsics
  ALWAYS_INLINE operator __m256i() const { return reg_; }

 protected:
  const __m256i &reg() const { return reg_; }

 protected:
  __m256i reg_;
};

class Vec4 : public Vec512b {
 public:
  Vec4() = default;
  Vec4(i64 val) noexcept { reg_ = _mm256_set1_epi64x(val); }
  Vec4(const __m256i &reg) noexcept : Vec512b(reg) {}
  Vec4(i64 val1, i64 val2, i64 val3, i64 val4) noexcept {
    reg_ = _mm256_setr_epi64x(val1, val2, val3, val4);
  }

  template <typename T>
  ALWAYS_INLINE inline void Load(const T *ptr) {
    reg_ = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr));
  }

  ALWAYS_INLINE inline void Store(i64 *ptr) const {
    _mm256_store_si256(reinterpret_cast<__m256i *>(ptr), reg());
  }

  ALWAYS_INLINE inline i64 Extract(u32 index) const {
    alignas(32) i64 x[Size()];
    Store(x);
    return x[index & 3];
  }

  ALWAYS_INLINE inline i64 operator[](u32 index) const {
    return Extract(index);
  }

  static constexpr u32 Size() { return 4; }
};

class Vec8 : public Vec512b {
 public:
  Vec8() = default;
  Vec8(i32 val) noexcept { reg_ = _mm256_set1_epi32(val); }
  Vec8(const __m256i &reg) noexcept : Vec512b(reg) {}
  Vec8(i32 val1, i32 val2, i32 val3, i32 val4, i32 val5, i32 val6, i32 val7,
       i32 val8) noexcept {
    reg_ = _mm256_setr_epi32(val1, val2, val3, val4, val5, val6, val7, val8);
  }

  template <typename T>
  void Load(const T *ptr);

  ALWAYS_INLINE inline void Store(i32 *ptr) const {
    _mm256_store_si256(reinterpret_cast<__m256i *>(ptr), reg());
  }

  ALWAYS_INLINE inline i32 Extract(u32 index) const {
    TPL_ASSERT(index < 8, "Out-of-bounds mask element access");
    alignas(32) i32 x[Size()];
    Store(x);
    return x[index & 7];
  }

  ALWAYS_INLINE inline i32 operator[](u32 index) const {
    return Extract(index);
  }

  static constexpr u32 Size() { return 8; }
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
    __m128i match_pos_scaled = _mm_loadl_epi64((__m128i *)&k8BitMatchLUT[mask]);
    __m256i match_pos = _mm256_cvtepi8_epi32(match_pos_scaled);
    __m256i pos_vec = _mm256_add_epi32(_mm256_set1_epi32(offset), match_pos);
    _mm256_storeu_si256(reinterpret_cast<__m256i *>(positions), pos_vec);
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
};

// Bit-wise NOT
static ALWAYS_INLINE inline Vec512b operator~(const Vec512b &v) noexcept {
  return _mm256_xor_si256(v, _mm256_set1_epi32(-1));
}

// Bit-wise AND
static ALWAYS_INLINE inline Vec512b operator&(const Vec512b &lhs,
                                              const Vec512b &rhs) noexcept {
  return _mm256_and_si256(lhs, rhs);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Vec8Mask
///
////////////////////////////////////////////////////////////////////////////////

static ALWAYS_INLINE inline Vec8Mask operator&(const Vec8Mask &lhs,
                                               const Vec8Mask &rhs) noexcept {
  return Vec8Mask(Vec512b(lhs) & Vec512b(rhs));
}

////////////////////////////////////////////////////////////////////////////////
///
/// Vec4Mask
///
////////////////////////////////////////////////////////////////////////////////

static ALWAYS_INLINE inline Vec4Mask operator&(const Vec4Mask &lhs,
                                               const Vec4Mask &rhs) noexcept {
  return Vec4Mask(Vec512b(lhs) & Vec512b(rhs));
}

////////////////////////////////////////////////////////////////////////////////
///
/// Vec8
///
////////////////////////////////////////////////////////////////////////////////

template <>
ALWAYS_INLINE inline void Vec8::Load<i8>(const i8 *ptr) {
  auto tmp = _mm_loadu_si128((const __m128i *)ptr);
  reg_ = _mm256_cvtepi8_epi32(tmp);
}

template <>
ALWAYS_INLINE inline void Vec8::Load<u8>(const u8 *ptr) {
  return Load<i8>(reinterpret_cast<const i8 *>(ptr));
}

template <>
ALWAYS_INLINE inline void Vec8::Load<i16>(const i16 *ptr) {
  auto tmp = _mm_loadu_si128((const __m128i *)ptr);
  reg_ = _mm256_cvtepi16_epi32(tmp);
}

template <>
ALWAYS_INLINE inline void Vec8::Load<u16>(const u16 *ptr) {
  return Load<i16>(reinterpret_cast<const i16 *>(ptr));
}

template <>
ALWAYS_INLINE inline void Vec8::Load<i32>(const i32 *ptr) {
  reg_ = _mm256_loadu_si256((const __m256i *)ptr);
}

template <>
ALWAYS_INLINE inline void Vec8::Load<u32>(const u32 *ptr) {
  return Load<i32>(reinterpret_cast<const i32 *>(ptr));
}

////////////////////////////////////////////////////////////////////////////////
///
/// Vec8 - Comparison Operations
///
////////////////////////////////////////////////////////////////////////////////

static ALWAYS_INLINE inline Vec8Mask operator>(const Vec8 &lhs,
                                               const Vec8 &rhs) noexcept {
  return _mm256_cmpgt_epi32(lhs, rhs);
}

static ALWAYS_INLINE inline Vec8Mask operator==(const Vec8 &lhs,
                                                const Vec8 &rhs) noexcept {
  return _mm256_cmpeq_epi32(lhs, rhs);
}

static ALWAYS_INLINE inline Vec8Mask operator>=(const Vec8 &lhs,
                                                const Vec8 &rhs) noexcept {
  __m256i max_lhs_rhs = _mm256_max_epu32(lhs, rhs);
  return _mm256_cmpeq_epi32(lhs, max_lhs_rhs);
}

static ALWAYS_INLINE inline Vec8Mask operator<(const Vec8 &lhs,
                                               const Vec8 &rhs) noexcept {
  return rhs > lhs;
}

static ALWAYS_INLINE inline Vec8Mask operator<=(const Vec8 &lhs,
                                                const Vec8 &rhs) noexcept {
  return rhs >= lhs;
}

static ALWAYS_INLINE inline Vec8Mask operator!=(const Vec8 &lhs,
                                                const Vec8 &rhs) noexcept {
  return Vec8Mask(~Vec512b(lhs == rhs));
}

////////////////////////////////////////////////////////////////////////////////
///
/// Vec8 - Arithmetic Operations
///
////////////////////////////////////////////////////////////////////////////////

static ALWAYS_INLINE inline Vec8 operator+(const Vec8 &lhs, const Vec8 &rhs) {
  return _mm256_add_epi32(lhs, rhs);
}

static ALWAYS_INLINE inline Vec8 operator-(const Vec8 &lhs, const Vec8 &rhs) {
  return _mm256_sub_epi32(lhs, rhs);
}

static ALWAYS_INLINE inline Vec8 &operator+=(Vec8 &lhs, const Vec8 &rhs) {
  lhs = lhs + rhs;
  return lhs;
}

////////////////////////////////////////////////////////////////////////////////
///
/// Vec4 - Comparison Operations
///
////////////////////////////////////////////////////////////////////////////////

static ALWAYS_INLINE inline Vec4Mask operator>(const Vec4 &lhs,
                                               const Vec4 &rhs) noexcept {
  return _mm256_cmpgt_epi64(lhs, rhs);
}

static ALWAYS_INLINE inline Vec4Mask operator==(const Vec4 &lhs,
                                                const Vec4 &rhs) noexcept {
  return _mm256_cmpeq_epi64(lhs, rhs);
}

static ALWAYS_INLINE inline Vec4Mask operator<(const Vec4 &lhs,
                                               const Vec4 &rhs) noexcept {
  return rhs > lhs;
}

static ALWAYS_INLINE inline Vec4Mask operator<=(const Vec4 &lhs,
                                                const Vec4 &rhs) noexcept {
  return Vec4Mask(~Vec512b(rhs > lhs));
}

static ALWAYS_INLINE inline Vec4Mask operator>=(const Vec4 &lhs,
                                                const Vec4 &rhs) noexcept {
  __m256i max_lhs_rhs = _mm256_max_epu64(lhs, rhs);
  return _mm256_cmpeq_epi64(lhs, max_lhs_rhs);
}

static ALWAYS_INLINE inline Vec4Mask operator!=(const Vec4 &lhs,
                                                const Vec4 &rhs) noexcept {
  return Vec4Mask(~Vec512b(lhs == rhs));
}

////////////////////////////////////////////////////////////////////////////////
///
/// Vec4 - Arithmetic Operations
///
////////////////////////////////////////////////////////////////////////////////

static ALWAYS_INLINE inline Vec4 operator+(const Vec4 &lhs, const Vec4 &rhs) {
  return _mm256_add_epi64(lhs, rhs);
}

static ALWAYS_INLINE inline Vec4 operator-(const Vec4 &lhs, const Vec4 &rhs) {
  return _mm256_sub_epi64(lhs, rhs);
}

static ALWAYS_INLINE inline Vec4 &operator+=(Vec4 &lhs, const Vec4 &rhs) {
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
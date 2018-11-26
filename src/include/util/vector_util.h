#pragma once

#include "util/common.h"
#include "util/simd.h"

namespace tpl::util {

class VectorUtil {
 private:
  template <typename T>
  struct GreaterEqual {
    using SimdCmp = simd::GreaterEqual;
    ALWAYS_INLINE inline bool operator()(const T &lhs, const T &rhs) const
        noexcept {
      return lhs >= rhs;
    }
  };

  template <typename T>
  struct Greater {
    using SimdCmp = simd::Greater;
    ALWAYS_INLINE inline bool operator()(const T &lhs, const T &rhs) const
        noexcept {
      return lhs > rhs;
    }
  };

  template <typename T>
  struct Equal {
    using SimdCmp = simd::Equal;
    ALWAYS_INLINE inline bool operator()(const T &lhs, const T &rhs) const
        noexcept {
      return lhs == rhs;
    }
  };

  template <typename T>
  struct Less {
    using SimdCmp = simd::Less;
    ALWAYS_INLINE inline bool operator()(const T &lhs, const T &rhs) const
        noexcept {
      return lhs < rhs;
    }
  };

  template <typename T>
  struct LessEqual {
    using SimdCmp = simd::LessEqual;
    ALWAYS_INLINE inline bool operator()(const T &lhs, const T &rhs) const
        noexcept {
      return lhs <= rhs;
    }
  };

  template <typename T>
  struct NotEqual {
    using SimdCmp = simd::NotEqual;
    ALWAYS_INLINE inline bool operator()(const T &lhs, const T &rhs) const
        noexcept {
      return lhs != rhs;
    }
  };

  template <typename T, typename CompareOp>
  static u32 Filter(const T *RESTRICT in, u32 in_count, T val,
                    u32 *RESTRICT out) {
    CompareOp op{};

    u32 in_pos = 0;
    u32 out_pos = 0;

    // SIMD first
    out_pos = simd::Filter<T, typename CompareOp::SimdCmp>(in, in_count, val,
                                                           out, in_pos);

    // Scalar tail
    for (; in_pos < in_count; in_pos++) {
      bool cmp = op(in[in_pos], val);
      out[out_pos] = in_pos;
      out_pos += static_cast<u32>(cmp);
    }

    return out_pos;
  }

 public:
  template <typename T>
  static u32 FilterLt(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out) {
    return Filter<T, Less<T>>(in, in_count, val, out);
  }

  template <typename T>
  static u32 FilterLe(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out) {
    return Filter<T, LessEqual<T>>(in, in_count, val, out);
  }

  template <typename T>
  static u32 FilterGt(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out) {
    return Filter<T, Greater<T>>(in, in_count, val, out);
  }

  template <typename T>
  static u32 FilterGe(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out) {
    return Filter<T, GreaterEqual<T>>(in, in_count, val, out);
  }

  template <typename T>
  static u32 FilterEq(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out) {
    return Filter<T, Equal<T>>(in, in_count, val, out);
  }

  template <typename T>
  static u32 FilterNe(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out) {
    return Filter<T, NotEqual<T>>(in, in_count, val, out);
  }
};

}  // namespace tpl::util
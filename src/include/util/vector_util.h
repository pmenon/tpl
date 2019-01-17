#pragma once

#include "util/common.h"
#include "util/simd.h"

namespace tpl::util {

class VectorUtil {
 public:
  template <typename T, typename Compare>
  static u32 Filter(const T *RESTRICT in, u32 in_count, T val,
                    u32 *RESTRICT out, u32 *RESTRICT sel) {
    const Compare op;

    u32 in_pos = 0;
    u32 out_pos = 0;

    if (sel == nullptr) {
      out_pos = simd::Filter<T, Compare>(in, in_count, val, out, sel, in_pos);

      for (; in_pos < in_count; in_pos++) {
        bool cmp = op(in[in_pos], val);
        out[out_pos] = in_pos;
        out_pos += static_cast<u32>(cmp);
      }
    } else {
      out_pos = simd::Filter<T, Compare>(in, in_count, val, out, sel, in_pos);

      for (; in_pos < in_count; in_pos++) {
        bool cmp = op(in[sel[in_pos]], val);
        out[out_pos] = sel[in_pos];
        out_pos += static_cast<u32>(cmp);
      }
    }

    return out_pos;
  }

  template <typename T>
  static u32 FilterGe(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out, u32 *RESTRICT sel) {
    return Filter<T, std::greater_equal<void>>(in, in_count, val, out, sel);
  }

  template <typename T>
  static u32 FilterGt(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out, u32 *RESTRICT sel) {
    return Filter<T, std::greater<void>>(in, in_count, val, out, sel);
  }

  template <typename T>
  static u32 FilterEq(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out, u32 *RESTRICT sel) {
    return Filter<T, std::equal_to<void>>(in, in_count, val, out, sel);
  }

  template <typename T>
  static u32 FilterLe(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out, u32 *RESTRICT sel) {
    return Filter<T, std::less_equal<void>>(in, in_count, val, out, sel);
  }

  template <typename T>
  static u32 FilterLt(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out, u32 *RESTRICT sel) {
    return Filter<T, std::less<void>>(in, in_count, val, out, sel);
  }

  template <typename T>
  static u32 FilterNe(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out, u32 *RESTRICT sel) {
    return Filter<T, std::not_equal_to<void>>(in, in_count, val, out, sel);
  }
};

}  // namespace tpl::util
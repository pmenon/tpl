#pragma once

#include "util/common.h"
#include "util/simd.h"

namespace tpl::util {

class VectorUtil {
 private:
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

 public:
  template <typename T>
  static u32 FilterGe(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out, u32 *RESTRICT sel) {
    return Filter<T, simd::GreaterEqual>(in, in_count, val, out, sel);
  }

  template <typename T>
  static u32 FilterGt(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out, u32 *RESTRICT sel) {
    return Filter<T, simd::Greater>(in, in_count, val, out, sel);
  }

  template <typename T>
  static u32 FilterEq(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out, u32 *RESTRICT sel) {
    return Filter<T, simd::Equal>(in, in_count, val, out, sel);
  }

  template <typename T>
  static u32 FilterLe(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out, u32 *RESTRICT sel) {
    return Filter<T, simd::LessEqual>(in, in_count, val, out, sel);
  }

  template <typename T>
  static u32 FilterLt(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out, u32 *RESTRICT sel) {
    return Filter<T, simd::Less>(in, in_count, val, out, sel);
  }

  template <typename T>
  static u32 FilterNe(const T *RESTRICT in, u32 in_count, T val,
                      u32 *RESTRICT out, u32 *RESTRICT sel) {
    return Filter<T, simd::NotEqual>(in, in_count, val, out, sel);
  }
};

}  // namespace tpl::util
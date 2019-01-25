#pragma once

#include "util/common.h"
#include "util/simd.h"

namespace tpl::util {

/// Catch-all utility class for vectorized operations
class VectorUtil {
 public:
  /// Filter an input vector by a constant value and store the indexes of valid
  /// elements in the output vector. If a selection vector is provided, only
  /// vector elements from the selection vector will be read.
  /// \tparam T The data type of the elements stored in the input vector
  /// \tparam Op The filter comparison operation
  /// \param in The input vector
  /// \param in_count The number of elements in the input (or selection) vector
  /// \param val The constant value to compare with
  /// \param[out] out The vector storing indexes of valid input elements
  /// \param sel The
  /// \return The number of elements that pass the filter
  template <typename T, typename Op>
  static u32 FilterVectorByVal(const T *RESTRICT in, u32 in_count, T val,
                               u32 *RESTRICT out, const u32 *RESTRICT sel) {
    const Op op;

    u32 in_pos = 0;
    u32 out_pos =
        simd::FilterVectorByVal<T, Op>(in, in_count, val, out, sel, in_pos);

    if (sel == nullptr) {
      for (; in_pos < in_count; in_pos++) {
        bool cmp = op(in[in_pos], val);
        out[out_pos] = in_pos;
        out_pos += static_cast<u32>(cmp);
      }
    } else {
      for (; in_pos < in_count; in_pos++) {
        bool cmp = op(in[sel[in_pos]], val);
        out[out_pos] = sel[in_pos];
        out_pos += static_cast<u32>(cmp);
      }
    }

    return out_pos;
  }

  // -------------------------------------------------------
  // Generate specialized vectorized filters
  // -------------------------------------------------------

#define GEN_FILTER(Op, Comparison)                                        \
  template <typename T>                                                   \
  static u32 Filter##Op(const T *RESTRICT in, u32 in_count, T val,        \
                        u32 *RESTRICT out, u32 *RESTRICT sel) {           \
    return FilterVectorByVal<T, Comparison>(in, in_count, val, out, sel); \
  }
  GEN_FILTER(Eq, std::equal_to<void>)
  GEN_FILTER(Gt, std::greater<void>)
  GEN_FILTER(Ge, std::greater_equal<void>)
  GEN_FILTER(Lt, std::less<void>)
  GEN_FILTER(Le, std::less_equal<void>)
  GEN_FILTER(Ne, std::not_equal_to<void>)
#undef GEN_FILTER
};

}  // namespace tpl::util
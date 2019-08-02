#pragma once

#include <functional>

#include "util/common.h"
#include "util/simd.h"

namespace tpl::util {

/**
 * Utility class containing vectorized operations.
 */
class VectorUtil {
 public:
  /**
   * Force only static functions.
   */
  VectorUtil() = delete;

  /**
   * Filter an input vector by a constant value and store the indexes of valid
   * elements in the output vector. If a selection vector is provided, only
   * vector elements from the selection vector will be read.
   * @tparam T The data type of the elements stored in the input vector.
   * @tparam Op The filter comparison operation.
   * @param in The input vector.
   * @param in_count The number of elements in the input (or selection) vector.
   * @param val The constant value to compare with.
   * @param[out] out The vector storing indexes of valid input elements.
   * @param sel The selection vector used to read input values.
   * @return The number of elements that pass the filter.
   */
  template <typename T, template <typename> typename Op>
  static u32 FilterVectorByVal(const T *RESTRICT in, const u32 in_count,
                               const T val, sel_t *RESTRICT out) {
    // Simple check to make sure the provided filter operation returns bool
    static_assert(std::is_same_v<bool, std::invoke_result_t<Op<T>, T, T>>);

    u32 in_pos = 0;
    u32 out_pos =
        simd::FilterVectorByVal<T, Op>(in, in_count, val, out, &in_pos);

    for (; in_pos < in_count; in_pos++) {
      bool cmp = Op<T>()(in[in_pos], val);
      out[out_pos] = in_pos;
      out_pos += static_cast<u32>(cmp);
    }

    return out_pos;
  }

  /**
   * Filter an input vector by the values in a second input vector, and store
   * indexes of the valid elements (zero-based) into an output vector. If a
   * selection vector is provided, only the vector elements whose indexes are in
   * the selection vector will be read.
   * @tparam T The data type of the elements stored in the input vector.
   * @tparam Op The filter operation.
   * @param in_1 The first input vector.
   * @param in_2 The second input vector.
   * @param in_count The number of elements in the input (or selection) vector.
   * @param[out] out The vector storing the indexes of the valid input elements.
   * @param sel The selection vector storing indexes of elements to process.
   * @return The number of elements that pass the filter.
   */
  template <typename T, template <typename> typename Op>
  static u32 FilterVectorByVector(const T *RESTRICT in_1,
                                  const T *RESTRICT in_2, const u32 in_count,
                                  sel_t *RESTRICT out) {
    // Simple check to make sure the provided filter operation returns bool
    static_assert(std::is_same_v<bool, std::invoke_result_t<Op<T>, T, T>>);

    u32 in_pos = 0;
    u32 out_pos =
        simd::FilterVectorByVector<T, Op>(in_1, in_2, in_count, out, &in_pos);

    for (; in_pos < in_count; in_pos++) {
      bool cmp = Op<T>()(in_1[in_pos], in_2[in_pos]);
      out[out_pos] = in_pos;
      out_pos += static_cast<u32>(cmp);
    }

    return out_pos;
  }

  // -------------------------------------------------------
  // Generate specialized vectorized filters
  // -------------------------------------------------------

#define GEN_FILTER(Op, Comparison)                                         \
  template <typename T>                                                    \
  static u32 Filter##Op(const T *RESTRICT in, u32 in_count, T val,         \
                        sel_t *RESTRICT out) {                             \
    return FilterVectorByVal<T, Comparison>(in, in_count, val, out);       \
  }                                                                        \
  template <typename T>                                                    \
  static u32 Filter##Op(const T *RESTRICT in_1, const T *RESTRICT in_2,    \
                        u32 in_count, sel_t *RESTRICT out) {               \
    return FilterVectorByVector<T, Comparison>(in_1, in_2, in_count, out); \
  }
  GEN_FILTER(Eq, std::equal_to)
  GEN_FILTER(Gt, std::greater)
  GEN_FILTER(Ge, std::greater_equal)
  GEN_FILTER(Lt, std::less)
  GEN_FILTER(Le, std::less_equal)
  GEN_FILTER(Ne, std::not_equal_to)
#undef GEN_FILTER

  /**
   * Convert a selection vector into a byte vector. For each index stored in the
   * selection vector, set the corresponding index in the byte vector to the
   * saturated 8-bit integer (0xFF = 255 = 11111111).
   * @param n The number of elements in the selection vector, and the minimum
   *          capacity of the byte vector.
   * @param sel_vector The input selection index vector.
   * @param[out] byte_vector The output byte vector.
   */
  static void SelectionVectorToByteVector(u32 n, const sel_t *sel_vector,
                                          u8 *byte_vector);

  /**
   * Convert a byte vector into a selection vector. For all elements in the byte
   * vector whose value is a saturated 8-bit integer (0xFF = 255 = 11111111),
   * left-pack the indexes of the elements into the selection vector.
   * @param n The number of elements in the byte vector, and the minimum
   *          capacity of the selection vector.
   * @param byte_vector The input byte vector.
   * @param[out] sel_vector The output selection vector.
   * @param[out] size The number of elements in the selection vector.
   */
  static void ByteVectorToSelectionVector(u32 n, const u8 *byte_vector,
                                          sel_t *sel_vector, u32 *size);

  /**
   * Convert a byte vector to a bit vector. For all elements in the byte vector
   * whose value is a saturated 8-bit integer (0xFF = 255 = 11111111), set the
   * corresponding bit in the bit vector to 1.
   * @param n The number of elements in the byte vector, and the minimum
   *          capacity (in bits) of the bit vector.
   * @param byte_vector The input byte vector.
   * @param[out] bit_vector The output bit vector.
   */
  static void ByteVectorToBitVector(u32 n, const u8 *byte_vector,
                                    u64 *bit_vector);

  /**
   * Perform a bitwise AND on all bytes in the byte vectors, storing the result
   * in @em byte_vector_2.
   * @param n The number of elements in both byte vectors.
   * @param byte_vector_1 The first input byte vector.
   * @param byte_vector_2 The second byte vector storing the result.
   */
  static void ByteVectorAnd(u32 n, const u8 *byte_vector_1, u8 *byte_vector_2);

  /**
   * Convert a bit vector into a densely packed selection vector. For all bits
   * in the bit vector that are true, insert their indexes into the output
   * selection vector. The resulting selection vector is guaranteed to be
   * sorted ascending.
   * @param n The number of bits in the bit vector, and the minimum capacity of
   *          the selection vector.
   * @param bit_vector The input bit vector.
   * @param[out] sel_vector The output selection vector.
   * @param[out] size The number of element in the selection vector.
   */
  static void BitVectorToSelectionVector(u32 n, const u64 *bit_vector,
                                         sel_t *sel_vector, u32 *size);
};

}  // namespace tpl::util

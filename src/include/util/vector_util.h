#pragma once

#include <functional>

// Needed for friend tests
#include "gtest/gtest_prod.h"

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
  [[nodiscard]] static u32 FilterVectorByVal(const T *RESTRICT in,
                                             const u32 in_count, const T val,
                                             sel_t *RESTRICT out) {
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
  [[nodiscard]] static u32 FilterVectorByVector(const T *RESTRICT in_1,
                                                const T *RESTRICT in_2,
                                                const u32 in_count,
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
  [[nodiscard]] static u32 Filter##Op(const T *RESTRICT in,                \
                                      const u32 in_count, const T val,     \
                                      sel_t *RESTRICT out) {               \
    return FilterVectorByVal<T, Comparison>(in, in_count, val, out);       \
  }                                                                        \
  template <typename T>                                                    \
  [[nodiscard]] static u32 Filter##Op(                                     \
      const T *RESTRICT in_1, const T *RESTRICT in_2, const u32 in_count,  \
      sel_t *RESTRICT out) {                                               \
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
   * Intersect the sorted input selection vectors @em v1 and @em v2, with
   * lengths @em v1_count and @em v2_count, respectively, and store the result
   * of the intersection in the output selection vector @em out_v.
   * @param sel_vector_1 The first input selection vector.
   * @param sel_vector_1_len The length of the first input selection vector.
   * @param sel_vector_2 The second input selection vector.
   * @param sel_vector_2_len The length of the second input selection vector.
   * @param out_sel_vector The output selection vector storing the result of the
   *              intersection.
   * @return The number of elements in the output selection vector.
   */
  [[nodiscard]] static u32
      IntersectSelected(const sel_t *sel_vector_1, u32 sel_vector_1_len,
                        const sel_t *sel_vector_2, u32 sel_vector_2_len,
                        sel_t *out_sel_vector);

  /**
   * Intersect the sorted input selection vector @em v1 and the input bit vector
   * @em bit_vector, and store the result of the intersection in the output
   * selection vector @em out_v.
   *
   * @param sel_vector The input selection vector.
   * @param sel_vector_len The length of the input selection vector.
   * @param bit_vector The input bit vector.
   * @param bit_vector_len The length of the bit vector in bits.
   * @param[out] out_sel_vector The output selection vector storing the result
   *                            of the intersection.
   * @return The number of elements in the output selection vector.
   */
  [[nodiscard]] static u32 IntersectSelected(const sel_t *sel_vector,
                                             u32 sel_vector_len,
                                             const u64 *bit_vector,
                                             u32 bit_vector_len,
                                             sel_t *out_sel_vector);

  /**
   * Populate the output selection vector @em out_sel_vector with all indexes
   * that do not appear in the input selection vector @em sel_vector.
   * @param n The maximum number of indexes that can appear in the selection
   *          vector.
   * @param sel_vector The input selection vector.
   * @param sel_vector_len The number of elements in the input selection vector.
   * @param[out] out_sel_vector The output selection vector.
   * @return The number of elements in the output selection vector.
   */
  [[nodiscard]] static u32 DiffSelected(u32 n, const sel_t *sel_vector,
                                        u32 sel_vector_len,
                                        sel_t *out_sel_vector);

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
   * @return The number of elements in the selection vector.
   */
  [[nodiscard]] static u32 ByteVectorToSelectionVector(u32 n,
                                                       const u8 *byte_vector,
                                                       sel_t *sel_vector);

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
   * Convert a bit vector into a byte vector. For all set bits in the input bit
   * vector, set the corresponding byte to a saturated 8-bit integer. The input
   * bit vector has @em n bits, and the output byte vector has @em n bytes.
   * @param n The number of bits in the bit vector, and the minimum capacity of
   *          the byte vector
   * @param bit_vector The input bit vector, passed along as an array of words.
   * @param byte_vector The output byte vector.
   */
  static void BitVectorToByteVector(u32 n, const u64 *bit_vector,
                                    u8 *byte_vector);

  /**
   * Convert a bit vector into a densely packed selection vector. For all bits
   * in the bit vector that are true, insert their indexes into the output
   * selection vector. The resulting selection vector is guaranteed to be
   * sorted ascending.
   * @param n The number of bits in the bit vector, and the minimum capacity of
   *          the selection vector.
   * @param bit_vector The input bit vector.
   * @param[out] sel_vector The output selection vector.
   * @return The number of elements in the selection vector.
   */
  [[nodiscard]] static u32 BitVectorToSelectionVector(u32 n,
                                                      const u64 *bit_vector,
                                                      sel_t *sel_vector);

 private:
  FRIEND_TEST(VectorUtilTest, IntersectScalar);
  FRIEND_TEST(VectorUtilTest, DiffSelected);
  FRIEND_TEST(VectorUtilTest, DiffSelectedWithScratchPad);
  FRIEND_TEST(VectorUtilTest, PerfIntersectSelected);

  // A sorted-set difference implementation using purely scalar operations
  [[nodiscard]] static u32 DiffSelected_Scalar(u32 n, const sel_t *sel_vector,
                                               u32 m, sel_t *out_sel_vector);

  // A sorted-set difference implementation that uses a little extra memory
  // (the scratchpad) and executes more instructions, but has better CPI, and is
  // faster in the common case.
  [[nodiscard]] static u32 DiffSelected_WithScratchPad(u32 n,
                                                       const sel_t *sel_vector,
                                                       u32 sel_vector_len,
                                                       sel_t *out_sel_vector,
                                                       u8 *scratch);
};

}  // namespace tpl::util

#pragma once

#include "common/common.h"
#include "common/macros.h"

namespace tpl::util {

/**
 * Utility class containing vectorized operations.
 */
class VectorUtil : public AllStatic {
 public:
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
  [[nodiscard]] static uint32_t IntersectSelected(const sel_t *sel_vector_1,
                                                  uint32_t sel_vector_1_len,
                                                  const sel_t *sel_vector_2,
                                                  uint32_t sel_vector_2_len, sel_t *out_sel_vector);

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
  [[nodiscard]] static uint32_t IntersectSelected(const sel_t *sel_vector, uint32_t sel_vector_len,
                                                  const uint64_t *bit_vector,
                                                  uint32_t bit_vector_len, sel_t *out_sel_vector);

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
  [[nodiscard]] static uint32_t DiffSelected(uint32_t n, const sel_t *sel_vector,
                                             uint32_t sel_vector_len, sel_t *out_sel_vector);

  /**
   * Convert a selection vector into a byte vector. For each index stored in the
   * selection vector, set the corresponding index in the byte vector to the
   * saturated 8-bit integer (0xFF = 255 = 11111111).
   * @param num_elems The number of elements in the selection vector, and the
   *                  minimum capacity of the byte vector.
   * @param sel_vector The input selection index vector.
   * @param[out] byte_vector The output byte vector.
   */
  static void SelectionVectorToByteVector(const sel_t *sel_vector, uint32_t num_elems,
                                          uint8_t *byte_vector);

  /**
   * Convert a byte vector into a selection vector. For all elements in the byte
   * vector whose value is a saturated 8-bit integer (0xFF = 255 = 11111111),
   * left-pack the indexes of the elements into the selection vector.
   * @param num_bytes The number of elements in the byte vector, and the minimum
   *                  capacity of the selection vector.
   * @param byte_vector The input byte vector.
   * @param[out] sel_vector The output selection vector.
   * @return The number of elements in the selection vector.
   */
  [[nodiscard]] static uint32_t ByteVectorToSelectionVector(const uint8_t *byte_vector,
                                                            uint32_t num_bytes, sel_t *sel_vector);

  /**
   * Convert a byte vector to a bit vector. For all elements in the byte vector
   * whose value is a saturated 8-bit integer (0xFF = 255 = 11111111), set the
   * corresponding bit in the bit vector to 1.
   * @param num_bytes The number of elements in the byte vector, and the minimum
   *                  capacity (in bits) of the bit vector.
   * @param byte_vector The input byte vector.
   * @param[out] bit_vector The output bit vector.
   */
  static void ByteVectorToBitVector(const uint8_t *byte_vector, uint32_t num_bytes,
                                    uint64_t *bit_vector);

  /**
   * Convert a bit vector into a byte vector. For all set bits in the input bit
   * vector, set the corresponding byte to a saturated 8-bit integer. The input
   * bit vector has @em n bits, and the output byte vector has @em n bytes.
   * @param num_bits The number of bits in the bit vector, and the minimum
   *                 capacity of the byte vector
   * @param bit_vector The input bit vector, passed along as an array of words.
   * @param byte_vector The output byte vector.
   */
  static void BitVectorToByteVector(const uint64_t *bit_vector, uint32_t num_bits,
                                    uint8_t *byte_vector);

  /**
   * Convert a bit vector into a densely packed selection vector. Extract the indexes of all set (1)
   * bits and store into the output selection vector. The resulting selection vector is guaranteed
   * to be sorted ascending.
   *
   * NOTE: Use this if you do not know the density of the bit vector. Otherwise, use the sparse or
   *       dense implementations below which are optimized as appropriate.
   *
   * @param bit_vector The input bit vector.
   * @param num_bits The number of bits in the bit vector. This must match the maximum capacity of
   *                 the output selection vector!
   * @param[out] sel_vector The output selection vector.
   * @return The number of elements in the selection vector.
   */
  [[nodiscard]] static uint32_t BitVectorToSelectionVector(const uint64_t *bit_vector,
                                                           uint32_t num_bits, sel_t *sel_vector);

  /**
   * Convert a bit vector into a densely packed selection vector using an algorithm optimized for
   * sparse bit vectors.
   *
   * @param bit_vector The input bit vector.
   * @param num_bits The number of bits in the bit vector. This must match the maximum capacity of
   *                 the output selection vector!
   * @param[out] sel_vector The output selection vector.
   * @return The number of elements in the selection vector.
   */
  [[nodiscard]] static uint32_t BitVectorToSelectionVector_Sparse(const uint64_t *bit_vector,
                                                                  uint32_t num_bits,
                                                                  sel_t *sel_vector);

  /**
   * Convert a bit vector into a densely packed selection vector using an algorithm optimized for
   * dense bit vectors.
   *
   * @param bit_vector The input bit vector.
   * @param num_bits The number of bits in the bit vector. This must match the maximum capacity of
   *                 the output selection vector!
   * @param[out] sel_vector The output selection vector.
   * @return The number of elements in the selection vector.
   */
  [[nodiscard]] static uint32_t BitVectorToSelectionVector_Dense(const uint64_t *bit_vector,
                                                                 uint32_t num_bits,
                                                                 sel_t *sel_vector);

 private:
  FRIEND_TEST(VectorUtilTest, IntersectScalar);
  FRIEND_TEST(VectorUtilTest, DiffSelected);
  FRIEND_TEST(VectorUtilTest, DiffSelectedWithScratchPad);
  FRIEND_TEST(VectorUtilTest, PerfIntersectSelected);

  [[nodiscard]] static uint32_t BitVectorToSelectionVector_Dense_AVX2(const uint64_t *bit_vector,
                                                                      uint32_t num_bits,
                                                                      sel_t *sel_vector);

  [[nodiscard]] static uint32_t BitVectorToSelectionVector_Dense_AVX512(const uint64_t *bit_vector,
                                                                        uint32_t num_bits,
                                                                        sel_t *sel_vector);

  // A sorted-set difference implementation using purely scalar operations
  [[nodiscard]] static uint32_t DiffSelected_Scalar(uint32_t n, const sel_t *sel_vector, uint32_t m,
                                                    sel_t *out_sel_vector);

  // A sorted-set difference implementation that uses a little extra memory
  // (the scratchpad) and executes more instructions, but has better CPI, and is
  // faster in the common case.
  [[nodiscard]] static uint32_t DiffSelected_WithScratchPad(uint32_t n, const sel_t *sel_vector,
                                                            uint32_t sel_vector_len,
                                                            sel_t *out_sel_vector,
                                                            uint8_t *scratch);
};

}  // namespace tpl::util

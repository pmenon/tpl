#pragma once

#include "sql/generic_value.h"
#include "sql/vector.h"
#include "util/common.h"

namespace tpl::sql {

/**
 * A utility class containing several core vectorized operations.
 */
class VectorOps {
 public:
  // Delete to force only static functions
  VectorOps() = delete;

  /**
   * Copy @em element_count elements from @em source starting at offset
   * @em offset into the (opaque) array @em target.
   * @param source The source vector to copy from.
   * @param target The target vector to copy into.
   * @param offset The index into the source vector to begin copying from.
   * @param element_count The number of elements to copy.
   */
  static void Copy(const Vector &source, void *target, u64 offset = 0,
                   u64 element_count = 0);

  /**
   * Copy all elements from @em source to the target vector @em target, starting
   * at offset @em offset in the source vector.
   * @param source The vector to copy from.
   * @param target The vector to copy into.
   * @param offset The offset in the source vector to begin reading.
   */
  static void Copy(const Vector &source, Vector *target, u64 offset = 0);

  /**
   * Cast all elements in the source vector @em source into elements of the
   * type the target vector @em target supports, and write them into the target
   * vector.
   * @param source The vector to cast from.
   * @param target The vector to cast and write into.
   */
  static void Cast(const Vector &source, Vector *target);

  /**
   * Cast all elements in the source vector @em source whose SQL type is
   * @em source_type into the target SQL type @em target_type and write the
   * results into the target vector @em target.
   * @param source The vector to read from.
   * @param target The vector to write into.
   * @param source_type The SQL type of elements in the source vector.
   * @param target_type The SQL type of elements in the target vector.
   */
  static void Cast(const Vector &source, Vector *target, SqlTypeId source_type,
                   SqlTypeId target_type);

  /**
   * Fill the input vector @em vector with sequentially increasing values
   * beginning at @em start and incrementing by @em increment.
   * @param vector The vector to fill.
   * @param start The first element to insert.
   * @param increment The amount to jump.
   */
  static void Generate(Vector *vector, i64 start, i64 increment);

  /**
   * Fill the input vector @em vector with a given non-null value @em value.
   * @param vector The vector to modify.
   * @param value The value to fill the vector with.
   */
  static void Fill(Vector *vector, const GenericValue &value);

  /**
   * Fill the input vector with NULL values.
   * @param vector The vector to modify.
   */
  static void FillNull(Vector *vector);

  // -------------------------------------------------------
  //
  // Comparisons
  //
  // -------------------------------------------------------

  /**
   * Perform an equality comparison on each element from the left and right
   * input vectors and store the result in the output vector @em result.
   * @param left The left input to the comparison.
   * @param right The right input to the comparison
   * @param[out] result The vector storing the result of the comparison.
   */
  static void Equal(const Vector &left, const Vector &right, Vector *result);

  /**
   * Perform a greater-than comparison on each element from the left and right
   * input vectors and store the result in the output vector @em result.
   * @param left The left input to the comparison.
   * @param right The right input to the comparison
   * @param[out] result The vector storing the result of the comparison.
   */
  static void GreaterThan(const Vector &left, const Vector &right,
                          Vector *result);

  /**
   * Perform a greater-than-or-equal comparison on each element from the left
   * and right input vectors and store the result in the output vector
   * @em result.
   * @param left The left input to the comparison.
   * @param right The right input to the comparison
   * @param[out] result The vector storing the result of the comparison.
   */
  static void GreaterThanEqual(const Vector &left, const Vector &right,
                               Vector *result);

  /**
   * Perform a less-than comparison on each element from the left and right
   * input vectors and store the result in the output vector @em result.
   * @param left The left input to the comparison.
   * @param right The right input to the comparison
   * @param[out] result The vector storing the result of the comparison.
   */
  static void LessThan(const Vector &left, const Vector &right, Vector *result);

  /**
   * Perform a less-than-or-equal comparison on each element from the left and
   * right input vectors and store the result in the output vector @em result.
   * @param left The left input to the comparison.
   * @param right The right input to the comparison
   * @param[out] result The vector storing the result of the comparison.
   */
  static void LessThanEqual(const Vector &left, const Vector &right,
                            Vector *result);

  /**
   * Perform an inequality comparison on each element from the left and right
   * input vectors and store the result in the output vector @em result.
   * @param left The left input to the comparison.
   * @param right The right input to the comparison
   * @param[out] result The vector storing the result of the comparison.
   */
  static void NotEqual(const Vector &left, const Vector &right, Vector *result);

  // -------------------------------------------------------
  //
  // Selection operations
  //
  // Selections are like comparisons, but generate a compact/compressed
  // selection index vector rather than a boolean match vector.
  //
  // -------------------------------------------------------

  /**
   * Store the positions of all equal elements in the left and right input
   * vectors into the output selection index vector.
   * @param left The left input into the comparison.
   * @param right The right input into the comparison.
   * @param[out] out_sel_vector The output selection index vector.
   * @param[out] out_count The number of items selected.
   */
  static void SelectEqual(const Vector &left, const Vector &right,
                          sel_t *out_sel_vector, u32 *out_count);

  /**
   * Store the positions where the left input element is strictly greater than
   * the element in the right input vector into the output selection index
   * vector.
   * @param left The left input into the comparison.
   * @param right The right input into the comparison.
   * @param[out] out_sel_vector The output selection index vector.
   * @param[out] out_count The number of items selected.
   */
  static void SelectGreaterThan(const Vector &left, const Vector &right,
                                sel_t *out_sel_vector, u32 *out_count);

  /**
   * Store the positions where the left input element is greater than or equal
   * to the element in the right input vector into the output selection index
   * vector.
   * @param left The left input into the comparison.
   * @param right The right input into the comparison.
   * @param[out] out_sel_vector The output selection index vector.
   * @param[out] out_count The number of items selected.
   */
  static void SelectGreaterThanEqual(const Vector &left, const Vector &right,
                                     sel_t *out_sel_vector, u32 *out_count);

  /**
   * Store the positions where the left input element is strictly less than the
   * element in the right input vector into the output selection index vector.
   * @param left The left input into the comparison.
   * @param right The right input into the comparison.
   * @param[out] out_sel_vector The output selection index vector.
   * @param[out] out_count The number of items selected.
   */
  static void SelectLessThan(const Vector &left, const Vector &right,
                             sel_t *out_sel_vector, u32 *out_count);

  /**
   * Store the positions where the left input element is less than or equal to
   * the element in the right input vector into the output selection index
   * vector.
   * @param left The left input into the comparison.
   * @param right The right input into the comparison.
   * @param[out] out_sel_vector The output selection index vector.
   * @param[out] out_count The number of items selected.
   */
  static void SelectLessThanEqual(const Vector &left, const Vector &right,
                                  sel_t *out_sel_vector, u32 *out_count);

  /**
   * Store the positions of all unequal elements in the left and right input
   * vectors into the output selection index vector.
   * @param left The left input into the comparison.
   * @param right The right input into the comparison.
   * @param[out] out_sel_vector The output selection index vector.
   * @param[out] out_count The number of items selected.
   */
  static void SelectNotEqual(const Vector &left, const Vector &right,
                             sel_t *out_sel_vector, u32 *out_count);

  // -------------------------------------------------------
  //
  // Boolean operations
  //
  // -------------------------------------------------------

  /**
   * Perform a boolean AND of the boolean elements in the left and right input
   * vectors and store the result in the output vector @em result.
   * @param left The left input.
   * @param right The right input.
   * @param[out] result The vector storing the result of the AND.
   */
  static void And(const Vector &left, const Vector &right, Vector *result);

  /**
   * Perform a boolean OR of the boolean elements in the left and right input
   * vectors and store the result in the output vector @em result.
   * @param left The left input.
   * @param right The right input.
   * @param[out] result The vector storing the result of the OR.
   */
  static void Or(const Vector &left, const Vector &right, Vector *result);

  /**
   * Perform a boolean negation of the boolean elements in the input vector and
   * store the result in the output vector @em result.
   * @param input The boolean input.
   * @param[out] result The vector storing the result of the AND.
   */
  static void Not(const Vector &input, Vector *result);

  // -------------------------------------------------------
  //
  // NULL checking
  //
  // -------------------------------------------------------

  /**
   * Check which elements of the vector @em input are NULL and store the results
   * in the boolean output vector @em result.
   * @param input The input vector whose elements are checked.
   * @param[out] result The output vector storing the results.
   */
  static void IsNull(const Vector &input, Vector *result);

  /**
   * Check which elements of the vector @em input are not NULL and store the
   * results in the boolean output vector @em result.
   * @param input The input vector whose elements are checked.
   * @param[out] result The output vector storing the results.
   */
  static void IsNotNull(const Vector &input, Vector *result);

  // -------------------------------------------------------
  //
  // Boolean checking
  //
  // -------------------------------------------------------

  /**
   * Check if every active element in the boolean input vector @em input is
   * non-null and true.
   * @param input The vector to check. Must be a boolean vector.
   * @return True if every element is non-null and true; false otherwise.
   */
  static bool AllTrue(const Vector &input);

  /**
   * Check if there is any active element in the boolean input vector @em input
   * that is both non-null and true.
   * @param input The vector to check. Must be a boolean vector.
   * @return True if every element is non-null and true; false otherwise.
   */
  static bool AnyTrue(const Vector &input);

  // -------------------------------------------------------
  //
  // Vector Iteration Logic
  //
  // -------------------------------------------------------

  /**
   * Apply a function to every active element in the vector. The callback
   * function will receive two indexes: i = index, dependent on the selection
   * vector, and k = count.
   */
  template <typename F>
  static void Exec(const sel_t *RESTRICT sel_vector, const u64 count, F &&fun,
                   const u64 offset = 0) {
    if (sel_vector != nullptr) {
      for (u64 i = offset; i < count; i++) {
        fun(sel_vector[i], i);
      }
    } else {
      for (u64 i = offset; i < count; i++) {
        fun(i, i);
      }
    }
  }

  /**
   * Apply a function to every active element in the vector. The callback
   * function will receive two arguments: i = index, dependent on the selection
   * vector, and k = count.
   */
  template <typename F>
  static void Exec(const Vector &vector, F &&fun, u64 offset = 0,
                   u64 count = 0) {
    if (count == 0) {
      count = vector.count_;
    } else {
      count += offset;
    }

    Exec(vector.sel_vector_, count, fun, offset);
  }

  /**
   * Apply a function to every active element in the vector. The callback
   * function will receive three arguments, val = the value of the element at
   * the current iteration position, i = index, dependent on the selection
   * vector, and k = count.
   */
  template <typename T, typename F>
  static void ExecTyped(const Vector &vector, F &&fun) {
    const auto *data = reinterpret_cast<const T *>(vector.data());
    Exec(vector.sel_vector_, vector.count_,
         [&](u64 i, u64 k) { fun(data[i], i, k); });
  }
};

}  // namespace tpl::sql

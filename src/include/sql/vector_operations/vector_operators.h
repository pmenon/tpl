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

  /**
   * Apply a function to every active element in the vector. The callback
   * function will receive two indexes: i = index, dependent on the selection
   * vector, and k = count.
   */
  template <typename T>
  static void Exec(const u32 *sel_vector, u64 count, T &&fun, u64 offset = 0) {
    // TODO(pmenon): Typically, these types of loops use the __restrict__
    //               on arrays to let the compiler know that two arrays (i.e.,
    //               pointer ranges) don't alias or overlap, thus allowing it
    //               to more aggresively optimize the loop. But, I don't know
    //               how that works with templates. Can we mark the loop with
    //               #pragma GCC ivdep ? And is that always true? Should (or can
    //               we) force callers to assert non-overlapping ranges?

    // If there's a selection vector, use it.
    if (sel_vector) {
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
  template <typename T>
  static void Exec(const Vector &vector, T &&fun, u64 offset = 0,
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
  static void ExecTyped(const Vector &vector, F &&fun, u64 offset = 0,
                        u64 count = 0) {
    auto data = reinterpret_cast<const T *>(vector.data());
    Exec(vector, [&](u64 i, u64 k) { fun(data[i], i, k); }, offset, count);
  }
};

}  // namespace tpl::sql

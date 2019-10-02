#pragma once

#include "common/common.h"
#include "sql/generic_value.h"
#include "sql/vector.h"

namespace tpl::sql {

class TupleIdList;

/**
 * A utility class containing several core vectorized operations.
 */
class VectorOps {
 public:
  // Delete to force only static functions
  VectorOps() = delete;

  /**
   * Copy @em element_count elements from @em source starting at offset @em offset into the (opaque)
   * array @em target.
   * @param source The source vector to copy from.
   * @param target The target vector to copy into.
   * @param offset The index into the source vector to begin copying from.
   * @param element_count The number of elements to copy.
   */
  static void Copy(const Vector &source, void *target, uint64_t offset = 0,
                   uint64_t element_count = 0);

  /**
   * Copy all elements from @em source to the target vector @em target, starting at offset
   * @em offset in the source vector.
   * @param source The vector to copy from.
   * @param target The vector to copy into.
   * @param offset The offset in the source vector to begin reading.
   */
  static void Copy(const Vector &source, Vector *target, uint64_t offset = 0);

  /**
   * Cast all elements in the source vector @em source into elements of the type the target vector
   * @em target supports, and write them into the target vector.
   * @param source The vector to cast from.
   * @param target The vector to cast and write into.
   */
  static void Cast(const Vector &source, Vector *target);

  /**
   * Cast all elements in the source vector @em source whose SQL type is @em source_type into the
   * target SQL type @em target_type and write the results into the target vector @em target.
   * @param source The vector to read from.
   * @param target The vector to write into.
   * @param source_type The SQL type of elements in the source vector.
   * @param target_type The SQL type of elements in the target vector.
   */
  static void Cast(const Vector &source, Vector *target, SqlTypeId source_type,
                   SqlTypeId target_type);

  /**
   * Fill the input vector @em vector with sequentially increasing values beginning at @em start and
   * incrementing by @em increment.
   * @param vector The vector to fill.
   * @param start The first element to insert.
   * @param increment The amount to jump.
   */
  static void Generate(Vector *vector, int64_t start, int64_t increment);

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
  // Selections
  //
  // -------------------------------------------------------

  /**
   * Filter the TID list @em tid_list with all elements in @em left that are equal to elements in
   * @em right.
   * @param left The left input into the selection.
   * @param right The right input into the selection
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void SelectEqual(const Vector &left, const Vector &right, TupleIdList *tid_list);

  /**
   * Filter the TID list @em tid_list with all elements in @em left that are strictly greater than
   * elements in @em right.
   * @param left The left input into the selection.
   * @param right The right input into the selection
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void SelectGreaterThan(const Vector &left, const Vector &right, TupleIdList *tid_list);

  /**
   * Filter the TID list @em tid_list with all elements in @em left that are greater than or equal
   * to elements @em right.
   * @param left The left input into the selection.
   * @param right The right input into the selection
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void SelectGreaterThanEqual(const Vector &left, const Vector &right,
                                     TupleIdList *tid_list);

  /**
   * Filter the TID list @em tid_list with all elements in @em left that are strictly less than
   * elements in @em right.
   * @param left The left input into the selection.
   * @param right The right input into the selection
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void SelectLessThan(const Vector &left, const Vector &right, TupleIdList *tid_list);

  /**
   * Filter the TID list @em tid_list with all elements in @em left that are less than or equal to
   * elements in @em right.
   * @param left The left input into the selection.
   * @param right The right input into the selection
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void SelectLessThanEqual(const Vector &left, const Vector &right, TupleIdList *tid_list);

  /**
   * Filter the TID list @em tid_list with all elements in @em left that are not equal to elements
   * in @em right.
   * @param left The left input into the selection.
   * @param right The right input into the selection
   * @param[in,out] tid_list The list of TIDs to read and update.
   */
  static void SelectNotEqual(const Vector &left, const Vector &right, TupleIdList *tid_list);

  // -------------------------------------------------------
  //
  // NULL check operations
  //
  // -------------------------------------------------------

  /**
   * Check TIDs in the list @em tid_list are NULL in the input vector @em input, storing the result
   * back into @em tid_list.
   * @param input The input vector whose elements are checked.
   * @param[in,out] tid_list The list of TIDs to check, and the output of the check.
   */
  static void IsNull(const Vector &input, TupleIdList *tid_list);

  /**
   * Check TIDs in the list @em tid_list are NOT NULL in the input vector @em input, storing the
   * result back into @em tid_list.
   * @param input The input vector whose elements are checked.
   * @param[in,out] tid_list The list of TIDs to check, and the output of the check.
   */
  static void IsNotNull(const Vector &input, TupleIdList *tid_list);

  // -------------------------------------------------------
  //
  // String operations
  //
  // -------------------------------------------------------

  /**
   * Store the TIDs of all string elements in @em a that are LIKE their counterparts in @em b in the
   * output Tuple ID list @em tid_list. Only elements whose TIDs appear in @em tid_list are
   * read and processed.
   * @param a The vector of strings to compare.
   * @param b The vector of strings to compare with.
   * @param[in,out] tid_list The list of TIDs to check, and the output of the check.
   */
  static void Like(const Vector &a, const Vector &b, TupleIdList *tid_list);

  /**
   * Store the TIDs of all string elements in @em a that are NOT LIKE their counterparts in @em b in
   * the output Tuple ID list @em tid_list. Only elements whose TIDs appear in @em tid_list are
   * read and processed.
   * @param a The vector of strings to compare.
   * @param b The vector of strings to compare with.
   * @param[in,out] tid_list The list of TIDs to check, and the output of the check.
   */
  static void NotLike(const Vector &a, const Vector &b, TupleIdList *tid_list);

  // -------------------------------------------------------
  //
  // Hashing
  //
  // -------------------------------------------------------

  /**
   * Hash vector elements from @em input into @em result.
   * @param input The input to hash.
   * @param[out] result The vector containing the hashed values.
   */
  static void Hash(const Vector &input, Vector *result);

  // -------------------------------------------------------
  //
  // Vector Iteration Logic
  //
  // -------------------------------------------------------

  /**
   * Apply a function to a range of indexes. If a selection vector is provided, the function @em fun
   * is applied to indexes from the selection vector in the range [offset, count). If a selection
   * vector is not provided, the function @em fun is applied to integers in the range
   * [offset, count).
   *
   * The callback function receives two parameters: i = the current index from the selection vector,
   * and k = count.
   *
   * @tparam F Functor accepting two integer arguments.
   * @param sel_vector The optional selection vector to iterate over.
   * @param count The number of elements in the selection vector if available.
   * @param fun The function to call on each element.
   * @param offset The offset from the beginning to begin iteration.
   */
  template <typename F>
  static void Exec(const sel_t *RESTRICT sel_vector, const uint64_t count, F &&fun,
                   const uint64_t offset = 0) {
    if (sel_vector != nullptr) {
      for (uint64_t i = offset; i < count; i++) {
        fun(sel_vector[i], i);
      }
    } else {
      for (uint64_t i = offset; i < count; i++) {
        fun(i, i);
      }
    }
  }

  /**
   * Apply a function to active elements in the input vector @em vector. If the vector has a
   * selection vector, the function @em fun is only applied to indexes from the selection vector in
   * the range [offset, count). If a selection vector is not provided, the function @em fun is
   * applied to integers in the range [offset, count).
   *
   * By default, the function will be applied to all active elements in the vector.
   *
   * The callback function receives two parameters: i = the current index from the selection vector,
   * and k = count.
   *
   * @tparam F Functor accepting two integer arguments.
   * @param sel_vector The optional selection vector to iterate over.
   * @param count The number of elements in the selection vector if available.
   * @param fun The function to call on each element.
   * @param offset The offset from the beginning to begin iteration.
   */
  template <typename F>
  static void Exec(const Vector &vector, F &&fun, uint64_t offset = 0, uint64_t count = 0) {
    if (count == 0) {
      count = vector.count_;
    } else {
      count += offset;
    }

    Exec(vector.sel_vector_, count, fun, offset);
  }

  /**
   * Apply a function to every active element in the vector. The callback function receives three
   * arguments, val = the value of the element at the current iteration position, i = index,
   * dependent on the selection vector, and k = count.
   */
  template <typename T, typename F>
  static void ExecTyped(const Vector &vector, F &&fun) {
    const auto *data = reinterpret_cast<const T *>(vector.GetData());
    Exec(vector.sel_vector_, vector.count_, [&](uint64_t i, uint64_t k) { fun(data[i], i, k); });
  }
};

}  // namespace tpl::sql

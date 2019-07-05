#pragma once

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
   *
   * @param vector
   * @param start
   * @param increment
   */
  static void Generate(Vector *vector, i64 start, i64 increment);

  /**
   * Apply a function to every active element in the vector. The callback
   * function will receive two indexes: i = index, dependent on the selection
   * vector, and k = count.
   */
  template <class T>
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
   * function will receive two indexes: i = index, dependent on the selection
   * vector, and k = count.
   */
  template <class T>
  static void Exec(const Vector &vector, T &&fun, u64 offset = 0,
                   u64 count = 0) {
    if (count == 0) {
      count = vector.count_;
    } else {
      count += offset;
    }

    Exec(vector.sel_vector_, count, fun, offset);
  }
};

}  // namespace tpl::sql

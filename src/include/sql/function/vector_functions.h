#pragma once

#include "sql/vector.h"
#include "util/common.h"

namespace tpl::sql {

class VectorFunctions {
 public:
  // Delete to force only static functions
  VectorFunctions() = delete;

  /**
   * Copy @em element_count elements from @em source starting at offset
   * @em offset into @em target.
   * @param source The source vector to copy from.
   * @param target The target vector to copy into.
   * @param offset The index into the source vector to begin copying from.
   * @param element_count The number of elements to copy.
   */
  static void Copy(const Vector &source, u8 *target, u64 offset = 0,
                   u64 element_count = 0);

  /**
   *
   * @param source
   * @param target
   * @param offset
   */
  static void Copy(const Vector &source, Vector *target, u64 offset = 0);

  static void Cast(const Vector &source, Vector *target);

  /**
   * Apply a function to every active element in the vector. The callback
   * function will receive two indexes: i = index, dependent on the selection
   * vector, and k = count.
   */
  template <class T>
  static void Exec(u32 *sel_vector, u64 count, T &&fun, u64 offset = 0) {
    u64 i = offset;
    if (sel_vector) {
      //#pragma GCC ivdep
      for (; i < count; i++) {
        fun(sel_vector[i], i);
      }
    } else {
      //#pragma GCC ivdep
      for (; i < count; i++) {
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
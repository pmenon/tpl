#pragma once

#include "sql/value.h"

namespace tpl::sql {

/**
 * Utility class to check NULL-ness of SQL values.
 */
class IsNullPredicate : public AllStatic {
 public:
  /**
   * Determine if the input value is a SQL NULL.
   * @param val The input value to check.
   * @return True if input is a SQL NULL; false otherwise.
   */
  static bool IsNull(const Val &val) { return val.is_null; }

  /**
   * Determine if the input value is NOT a SQL NULL.
   * @param val The input value to check.
   * @return True if input is not a SQL NULL; false otherwise.
   */
  static bool IsNotNull(const Val &val) { return !val.is_null; }
};

}  // namespace tpl::sql

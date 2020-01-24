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
   * @param[out] result Result of the NULL check. True if input is NULL; false otherwise.
   * @param val The input value to check.
   */
  static void IsNull(bool *result, const Val &val) { *result = val.is_null; }

  /**
   * Determine if the input value is NOT a SQL NULL.
   * @param[out] result Result of the NULL check. True if input is not NULL; false otherwise.
   * @param val The input value to check.
   */
  static void IsNotNull(bool *result, const Val &val) { *result = val.is_null; }
};

}  // namespace tpl::sql

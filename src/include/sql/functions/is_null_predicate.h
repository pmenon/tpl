#pragma once

#include "sql/value.h"

namespace tpl::sql {

/**
 * Utility class to check NULL-ness of SQL values.
 */
class IsNullPredicate : public AllStatic {
 public:
  /**
   * Determine if the input value @em val is NULL, setting the result to true if so.
   * @param[out] result Result of the NULL check. True if @em val is NULL; false otherwise.
   * @param val The input value to check.
   */
  static void IsNull(BoolVal *result, const Val &val) { *result = BoolVal(val.is_null); }

  /**
   * Determine if the input value @em val is not NULL, setting the result to true if so.
   * @param[out] result Result of the NULL check. True if @em val is not NULL; false otherwise.
   * @param val The input value to check.
   */
  static void IsNotNull(BoolVal *result, const Val &val) { *result = BoolVal(!val.is_null); }
};

}  // namespace tpl::sql

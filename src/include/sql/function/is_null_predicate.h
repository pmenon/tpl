#pragma once

#include "sql/value.h"

namespace tpl::sql {

/**
 * Utility class to check NULL-ness of SQL values.
 */
class IsNullPredicate {
 public:
  // Delete to force only static functions
  IsNullPredicate() = delete;

  static void IsNull(BoolVal *result, const Val &val) {
    *result = BoolVal(val.is_null);
  }

  static void IsNotNull(BoolVal *result, const Val &val) {
    *result = BoolVal(!val.is_null);
  }
};

}  // namespace tpl::sql

#include "ast/type.h"

namespace tpl::ast {

bool Type::IsArithmetic() const {
  if (IsIntegerType() || IsFloatType()) {
    return true;
  }
  if (auto *type = SafeAs<SqlType>()) {
    return type->sql_type().IsArithmetic();
  }
  return false;
}

}  // namespace tpl::ast
#include "ast/ast.h"

namespace tpl::ast {

FunctionType *FunctionDeclaration::type() { return fun_->type(); }

}  // namespace tpl::ast
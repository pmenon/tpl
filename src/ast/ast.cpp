#include "ast/ast.h"

namespace tpl {

FunctionType *FunctionDeclaration::type() { return fun_->type(); }

}  // namespace tpl
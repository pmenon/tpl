#include "ast/ast.h"

namespace tpl {

const FunctionType *FunctionDeclaration::type() const { return fun_->type(); }

}  // namespace tpl
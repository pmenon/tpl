#include "sema/sema.h"

#include "ast/ast_context.h"
#include "ast/type.h"

namespace tpl::sema {

void Sema::VisitVariableDeclaration(ast::VariableDeclaration *node) {
  if (current_scope()->LookupLocal(node->name()) != nullptr) {
    error_reporter().Report(node->position(),
                            ErrorMessages::kVariableRedeclared, node->name());
    return;
  }

  // At this point, the variable either has a declared type or an initial value
  TPL_ASSERT(node->type_repr() != nullptr || node->initial() != nullptr,
             "Variable has neither a type declaration or an initial "
             "expression. This should have been caught during parsing.");

  ast::Type *declared_type = nullptr;
  ast::Type *initializer_type = nullptr;

  if (node->type_repr() != nullptr) {
    declared_type = Resolve(node->type_repr());
  }

  if (node->initial() != nullptr) {
    initializer_type = Resolve(node->initial());
  }

  if (declared_type == nullptr && initializer_type == nullptr) {
    // Error
    return;
  }

  if (declared_type != nullptr && initializer_type != nullptr) {
    // Check compatibility
  }

  // The type should be resolved now
  current_scope()->Declare(
      node, (declared_type != nullptr ? declared_type : initializer_type));
}

void Sema::VisitFieldDeclaration(ast::FieldDeclaration *node) {
  Visit(node->type_repr());
}

void Sema::VisitFunctionDeclaration(ast::FunctionDeclaration *node) {
  auto *func_type = Resolve(node->function());

  if (func_type == nullptr) {
    return;
  }

  current_scope()->Declare(node, func_type);
}

void Sema::VisitStructDeclaration(ast::StructDeclaration *node) {
  auto *struct_type = Resolve(node->type_repr());

  if (struct_type == nullptr) {
    return;
  }

  current_scope()->Declare(node, struct_type);
}

}  // namespace tpl::sema
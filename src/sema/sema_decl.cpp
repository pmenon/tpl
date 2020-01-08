#include "sema/sema.h"

#include <unordered_set>

#include "ast/context.h"
#include "ast/type.h"

namespace tpl::sema {

void Sema::VisitVariableDecl(ast::VariableDecl *node) {
  if (current_scope()->LookupLocal(node->Name()) != nullptr) {
    error_reporter()->Report(node->Position(), ErrorMessages::kVariableRedeclared, node->Name());
    return;
  }

  // At this point, the variable either has a declared type or an initial value
  TPL_ASSERT(node->HasTypeDecl() || node->HasInitialValue(),
             "Variable has neither a type declaration or an initial "
             "expression. This should have been caught during parsing.");

  ast::Type *declared_type = nullptr;
  ast::Type *initializer_type = nullptr;

  if (node->HasTypeDecl()) {
    declared_type = Resolve(node->TypeRepr());
  }

  if (node->HasInitialValue()) {
    initializer_type = Resolve(node->Initial());
  }

  if (declared_type == nullptr && initializer_type == nullptr) {
    return;
  }

  // If both are provided, check assignment
  if (declared_type != nullptr && initializer_type != nullptr) {
    ast::Expr *init = node->Initial();
    if (!CheckAssignmentConstraints(declared_type, init)) {
      error_reporter()->Report(node->Position(), ErrorMessages::kInvalidAssignment, declared_type,
                               initializer_type);
      return;
    }
    // If the check applied an implicit cast, reset the initializing expression
    if (init != node->Initial()) {
      node->SetInitial(init);
    }
  }

  // The type should be resolved now
  current_scope()->Declare(node->Name(),
                           (declared_type != nullptr ? declared_type : initializer_type));
}

void Sema::VisitFieldDecl(ast::FieldDecl *node) { Visit(node->TypeRepr()); }

void Sema::VisitFunctionDecl(ast::FunctionDecl *node) {
  // Resolve just the function type (not the body of the function)
  auto *func_type = Resolve(node->TypeRepr());

  if (func_type == nullptr) {
    return;
  }

  // Check for duplicate fields.
  std::unordered_set<ast::Identifier> seen_fields;
  for (const auto *field : node->TypeRepr()->As<ast::FunctionTypeRepr>()->Parameters()) {
    if (seen_fields.count(field->Name()) > 0) {
      error_reporter()->Report(node->Position(), ErrorMessages::kDuplicateArgName,
                               field->Name(), node->Name());
      return;
    }
    seen_fields.insert(field->Name());
  }

  // Make declaration available.
  current_scope()->Declare(node->Name(), func_type);

  // Now resolve the whole function.
  Resolve(node->Function());
}

void Sema::VisitStructDecl(ast::StructDecl *node) {
  auto *struct_type = Resolve(node->TypeRepr());

  if (struct_type == nullptr) {
    return;
  }

  // Check for duplicate fields.
  std::unordered_set<ast::Identifier> seen_fields;
  for (const auto *field : node->TypeRepr()->As<ast::StructTypeRepr>()->Fields()) {
    if (seen_fields.count(field->Name()) > 0) {
      error_reporter()->Report(node->Position(), ErrorMessages::kDuplicateStructFieldName,
                               field->Name(), node->Name());
      return;
    }
    seen_fields.insert(field->Name());
  }

  // Make the declaration available.
  current_scope()->Declare(node->Name(), struct_type);
}

}  // namespace tpl::sema

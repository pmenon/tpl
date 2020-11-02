#include "sema/sema.h"

#include "llvm/ADT/DenseSet.h"

#include "ast/context.h"
#include "ast/type.h"
#include "sema/error_reporter.h"

namespace tpl::sema {

void Sema::VisitVariableDecl(ast::VariableDecl *node) {
  TPL_ASSERT(scope_ != nullptr, "No scope exists!");
  if (scope_->LookupLocal(node->Name()) != nullptr) {
    error_reporter_->Report(node->Position(), ErrorMessages::kVariableRedeclared, node->Name());
    return;
  }

  // At this point, the variable either has a declared type or an initial value.
  TPL_ASSERT(node->HasTypeDecl() || node->HasInitialValue(),
             "Variable has neither a type declaration or an initial expression. This should have "
             "been caught during parsing.");

  ast::Type *declared_type = nullptr, *initializer_type = nullptr;

  if (node->HasTypeDecl()) {
    declared_type = Resolve(node->TypeRepr());
  }

  if (node->HasInitialValue()) {
    initializer_type = Resolve(node->Initial());
  }

  // If neither are resolved, it's an error.
  if (declared_type == nullptr && initializer_type == nullptr) {
    return;
  }

  if (declared_type != nullptr && initializer_type != nullptr) {
    // If both type declarations are provided, check assignment.
    ast::Expr *init = node->Initial();
    if (!CheckAssignmentConstraints(declared_type, init)) {
      error_reporter_->Report(node->Position(), ErrorMessages::kInvalidAssignment, declared_type,
                              initializer_type);
      return;
    }
    // If the check applied an implicit cast, reset the initializing expression.
    if (init != node->Initial()) {
      node->SetInitial(init);
    }
  } else if (initializer_type != nullptr) {
    // Both type declarations are not provided, but the initial value has a
    // resolved type. Let's check it now.
    if (initializer_type->IsNilType()) {
      error_reporter_->Report(node->Position(), ErrorMessages::kUseOfUntypedNil);
      return;
    }
  }

  TPL_ASSERT(scope_ != nullptr, "No scope exists!");
  ast::Type *resolved_type = (declared_type != nullptr ? declared_type : initializer_type);
  scope_->Declare(node->Name(), resolved_type);
}

void Sema::VisitFieldDecl(ast::FieldDecl *node) { Visit(node->TypeRepr()); }

namespace {

// Return true if the given list of field declarations contains a duplicate name. If so, the 'dup'
// output parameter is set to the offending field. Otherwise, return false;
bool HasDuplicatesNames(const util::RegionVector<ast::FieldDecl *> &fields,
                        const ast::FieldDecl **dup) {
  llvm::SmallDenseSet<ast::Identifier, 32> seen;
  for (const auto *field : fields) {
    // Attempt to insert into the set. If the insertion succeeds it's a unique
    // name. If the insertion fails, it's a duplicate name.
    const bool inserted = seen.insert(field->Name()).second;
    if (!inserted) {
      *dup = field;
      return true;
    }
  }
  return false;
}

}  // namespace

void Sema::VisitFunctionDecl(ast::FunctionDecl *node) {
  // Resolve **JUST** the function's type representation, not the function body.
  ast::Type *func_type = Resolve(node->TypeRepr());

  // Error.
  if (func_type == nullptr) {
    return;
  }

  // At this point, the resolved type should be a function type.
  // The FunctionDecl constructor forces this. But, it's 2020, so let's be sure.
  TPL_ASSERT(func_type->IsFunctionType(), "Resolved type isn't function?!");

  // Check for duplicate parameter names in signature.
  if (const ast::FieldDecl *dup = nullptr;
      HasDuplicatesNames(node->TypeRepr()->As<ast::FunctionTypeRepr>()->Parameters(), &dup)) {
    error_reporter_->Report(node->Position(), ErrorMessages::kDuplicateArgName, dup->Name(),
                            node->Name());
    return;
  }

  // Make declaration available.
  TPL_ASSERT(scope_ != nullptr, "No scope exists!");
  scope_->Declare(node->Name(), func_type);

  // Now resolve the whole function.
  Resolve(node->Function());
}

void Sema::VisitStructDecl(ast::StructDecl *node) {
  ast::Type *struct_type = Resolve(node->TypeRepr());

  if (struct_type == nullptr) {
    return;
  }

  // At this point, the resolved type should be a struct type.
  // The StructDecl constructor forces this. But, it's 2020, so let's be sure.
  TPL_ASSERT(struct_type->IsStructType(), "Resolved type isn't struct?!");

  // Check for duplicate fields.
  if (const ast::FieldDecl *dup = nullptr;
      HasDuplicatesNames(node->TypeRepr()->As<ast::StructTypeRepr>()->Fields(), &dup)) {
    error_reporter_->Report(node->Position(), ErrorMessages::kDuplicateStructFieldName, dup->Name(),
                            node->Name());
    return;
  }

  // Make the declaration available.
  TPL_ASSERT(scope_ != nullptr, "No scope exists!");
  scope_->Declare(node->Name(), struct_type);
}

}  // namespace tpl::sema

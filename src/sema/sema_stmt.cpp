#include "sema/sema.h"

#include "ast/ast_node_factory.h"
#include "ast/context.h"
#include "ast/type.h"
#include "sema/error_reporter.h"
#include "sql/catalog.h"
#include "sql/table.h"

namespace tpl::sema {

void Sema::VisitAssignmentStatement(ast::AssignmentStatement *node) {
  ast::Type *src_type = Resolve(node->GetSource());
  ast::Type *dest_type = Resolve(node->GetDestination());

  if (src_type == nullptr || dest_type == nullptr) {
    return;
  }

  // Check assignment
  ast::Expr *source = node->GetSource();
  if (!CheckAssignmentConstraints(dest_type, source)) {
    error_reporter_->Report(node->Position(), ErrorMessages::kInvalidAssignment, src_type,
                            dest_type);
    return;
  }

  // Assignment looks good, but the source may have been casted
  if (source != node->GetSource()) {
    node->SetSource(source);
  }
}

void Sema::VisitBlockStatement(ast::BlockStatement *node) {
  SemaScope block_scope(this, Scope::Kind::Block);

  for (auto *stmt : node->GetStatements()) {
    Visit(stmt);
  }
}

void Sema::VisitFile(ast::File *node) {
  SemaScope file_scope(this, Scope::Kind::File);

  for (auto *decl : node->GetDeclarations()) {
    Visit(decl);
  }
}

void Sema::VisitForStatement(ast::ForStatement *node) {
  // Create a new scope for variables introduced in initialization block
  SemaScope for_scope(this, Scope::Kind::Loop);

  if (node->GetInit() != nullptr) {
    Visit(node->GetInit());
  }

  if (node->GetCondition() != nullptr) {
    ast::Type *cond_type = Resolve(node->GetCondition());
    // If unable to resolve condition type, there was some error
    if (cond_type == nullptr) {
      return;
    }
    // If the resolved type isn't a boolean, it's an error
    if (!cond_type->IsBoolType()) {
      error_reporter_->Report(node->GetCondition()->Position(),
                              ErrorMessages::kNonBoolForCondition);
    }
  }

  if (node->GetNext() != nullptr) {
    Visit(node->GetNext());
  }

  // The body
  Visit(node->GetBody());
}

void Sema::VisitForInStatement(ast::ForInStatement *node) { TPL_ASSERT(false, "Not supported"); }

void Sema::VisitExpressionStatement(ast::ExpressionStatement *node) {
  Visit(node->GetExpression());
}

void Sema::VisitIfStatement(ast::IfStatement *node) {
  if (ast::Type *cond_type = Resolve(node->GetCondition()); cond_type == nullptr) {
    // Error
    return;
  }

  // If the result type of the evaluated condition is a SQL boolean value, we
  // implicitly cast it to a native boolean value before we feed it into the
  // if-condition.

  if (node->GetCondition()->GetType()->IsSpecificBuiltin(ast::BuiltinType::BooleanVal)) {
    // A primitive boolean
    auto *bool_type = ast::BuiltinType::Get(context_, ast::BuiltinType::Bool);

    // Perform implicit cast from SQL boolean to primitive boolean
    ast::Expr *cond = node->GetCondition();
    cond = context_->GetNodeFactory()->NewImplicitCastExpr(
        cond->Position(), ast::CastKind::SqlBoolToBool, bool_type, cond);
    cond->SetType(bool_type);
    node->SetCondition(cond);
  }

  // If the conditional isn't an explicit boolean type, error
  if (!node->GetCondition()->GetType()->IsBoolType()) {
    error_reporter_->Report(node->GetCondition()->Position(), ErrorMessages::kNonBoolIfCondition);
  }

  Visit(node->GetThenStatement());

  if (node->GetElseStatement() != nullptr) {
    Visit(node->GetElseStatement());
  }
}

void Sema::VisitDeclarationStatement(ast::DeclarationStatement *node) {
  Visit(node->GetDeclaration());
}

void Sema::VisitReturnStatement(ast::ReturnStatement *node) {
  if (GetCurrentFunction() == nullptr) {
    error_reporter_->Report(node->Position(), ErrorMessages::kReturnOutsideFunction);
    return;
  }

  // If there's an expression with the return clause, resolve it now. We'll
  // check later if we need it.

  ast::Type *return_type = nullptr;
  if (node->GetReturnValue() != nullptr) {
    return_type = Resolve(node->GetReturnValue());
  }

  // If the function has a nil-type, we just need to make sure this return
  // statement doesn't have an attached expression. If it does, that's an error

  auto *func_type = GetCurrentFunction()->GetType()->As<ast::FunctionType>();

  if (func_type->GetReturnType()->IsNilType()) {
    if (return_type != nullptr) {
      error_reporter_->Report(node->Position(), ErrorMessages::kMismatchedReturnType, return_type,
                              func_type->GetReturnType());
    }
    return;
  }

  // The function has a non-nil return type. So, we need to make sure the
  // resolved type of the expression in this return is compatible with the
  // return type of the function.

  if (return_type == nullptr) {
    error_reporter_->Report(node->Position(), ErrorMessages::kMissingReturn);
    return;
  }

  ast::Expr *ret = node->GetReturnValue();
  if (!CheckAssignmentConstraints(func_type->GetReturnType(), ret)) {
    error_reporter_->Report(node->Position(), ErrorMessages::kMismatchedReturnType, return_type,
                            func_type->GetReturnType());
    return;
  }

  if (ret != node->GetReturnValue()) {
    node->SetReturnValue(ret);
  }
}

}  // namespace tpl::sema

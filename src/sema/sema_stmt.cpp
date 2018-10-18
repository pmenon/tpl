#include "sema/sema.h"

#include "ast/ast_context.h"
#include "ast/type.h"
#include "runtime/sql_table.h"

namespace tpl::sema {

void Sema::VisitAssignmentStmt(ast::AssignmentStmt *node) {
  auto *src_type = Resolve(node->source());
  auto *dest_type = Resolve(node->destination());

  if (src_type == nullptr || dest_type == nullptr) {
    // Skip
  }

  if (src_type != dest_type) {
    // Error
  }
}

void Sema::VisitBlockStmt(ast::BlockStmt *node) {
  SemaScope block_scope(*this, Scope::Kind::Block);

  for (auto *stmt : node->statements()) {
    Visit(stmt);
  }
}

void Sema::VisitFile(ast::File *node) {
  SemaScope file_scope(*this, Scope::Kind::File);

  for (auto *decl : node->declarations()) {
    Visit(decl);
  }
}

void Sema::VisitForStmt(ast::ForStmt *node) {
  // Create a new scope for variables introduced in initialization block
  SemaScope for_scope(*this, Scope::Kind::Loop);

  if (node->init() != nullptr) {
    Visit(node->init());
  }

  if (node->condition() != nullptr) {
    ast::Type *cond_type = Resolve(node->condition());
    if (!cond_type->IsBoolType()) {
      error_reporter().Report(node->condition()->position(),
                              ErrorMessages::kNonBoolForCondition);
    }
  }

  if (node->next() != nullptr) {
    Visit(node->next());
  }

  // The body
  Visit(node->body());
}

void Sema::VisitForInStmt(ast::ForInStmt *node) {
  SemaScope for_scope(*this, Scope::Kind::Loop);

  if (!node->target()->IsIdentifierExpr()) {
    error_reporter().Report(node->target()->position(),
                            ErrorMessages::kNonIdentifierTargetInForInLoop);
    return;
  }

  if (!node->iter()->IsIdentifierExpr()) {
    error_reporter().Report(node->iter()->position(),
                            ErrorMessages::kNonIdentifierIterator);
    return;
  }

  auto *target = node->target()->As<ast::IdentifierExpr>();
  auto *iter = node->iter()->As<ast::IdentifierExpr>();

  // Lookup the table in the catalog
  // TODO(pmenon): This will change after we integrate with bigger system
  auto *table = runtime::LookupTableByName(iter->name().data());
  if (table == nullptr) {
    error_reporter().Report(iter->position(), ErrorMessages::kNonExistingTable,
                            iter->name());
    return;
  }

  // Convert the table schema into a TPL struct type
  auto *row_type = ConvertToType(table->schema());
  TPL_ASSERT(row_type->IsStructType(), "Rows must be structs");

  // Set the target's type
  target->set_type(row_type);

  // Declare iteration variable
  current_scope()->Declare(target->name(), row_type);

  // Process body
  Visit(node->body());
}

void Sema::VisitExpressionStmt(ast::ExpressionStmt *node) {
  Visit(node->expression());
}

void Sema::VisitIfStmt(ast::IfStmt *node) {
  ast::Type *cond_type = Resolve(node->condition());

  if (cond_type == nullptr) {
    // Error
    return;
  }

  if (!cond_type->IsBoolType()) {
    error_reporter().Report(node->condition()->position(),
                            ErrorMessages::kNonBoolIfCondition);
  }

  Visit(node->then_stmt());

  if (node->else_stmt() != nullptr) {
    Visit(node->else_stmt());
  }
}

void Sema::VisitDeclStmt(ast::DeclStmt *node) { Visit(node->declaration()); }

void Sema::VisitReturnStmt(ast::ReturnStmt *node) {
  if (current_function() == nullptr) {
    error_reporter().Report(node->position(),
                            ErrorMessages::kReturnOutsideFunction);
    return;
  }

  ast::Type *return_type = Resolve(node->ret());
  if (return_type == nullptr) {
    return;
  }

  // Check return type matches function's return type
  auto *func_type = current_function()->type()->As<ast::FunctionType>();
  if (return_type != func_type->return_type()) {
    error_reporter().Report(node->position(),
                            ErrorMessages::kMismatchedReturnType, return_type,
                            func_type->return_type());
    return;
  }
}

}  // namespace tpl::sema
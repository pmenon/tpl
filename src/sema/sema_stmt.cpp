#include "sema/sema.h"

#include "ast/ast_context.h"
#include "ast/type.h"

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
  // Create a block scope
  SemaScope block_scope(*this, Scope::Kind::Block);

  for (auto *stmt : node->statements()) {
    Visit(stmt);
  }
}

void Sema::VisitFile(ast::File *node) {
  // Create RAII file scope
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

  ast::Type *ret = Resolve(node->ret());
  if (ret == nullptr) {
    return;
  }

  // Check return type matches function
  auto *func_type = current_function()->type()->As<ast::FunctionType>();
  if (ret != func_type->return_type()) {
    // Error
  }
}

}  // namespace tpl::sema
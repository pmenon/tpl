#include "sema/type_check.h"

namespace tpl::sema {

TypeChecker::TypeChecker(util::Region &region, ErrorReporter &error_reporter)
    : region_(region), error_reporter_(error_reporter) {}

bool TypeChecker::Run(ast::AstNode *root) {
  Visit(root);
  return error_reporter().has_errors();
}

void TypeChecker::VisitBadExpression(ast::BadExpression *node) {}

void TypeChecker::VisitUnaryExpression(ast::UnaryExpression *node) {
  Visit(node->expr());

  ast::Type *expr_type = node->expr()->type();

  if (expr_type == nullptr) {
    return;
  }

  switch (node->op()) {
    case parsing::Token::BANG: {
      if (expr_type->IsBoolType()) {
        node->set_type(expr_type);
      } else {
        ReportError(node->position(), ErrorMessages::kInvalidOperation,
                    node->op(), expr_type->name());
      }
      break;
    }
    case parsing::Token::MINUS: {
      if (!expr_type->IsNumber()) {
        ReportError(node->position(), ErrorMessages::kInvalidOperation,
                    node->op(), expr_type->name());
      } else {
        node->set_type(expr_type);
      }
      break;
    }
    case parsing::Token::Type::STAR: {
      if (!expr_type->IsPointerType()) {
        ReportError(node->position(), ErrorMessages::kInvalidOperation,
                    node->op(), expr_type->name());
      } else {
        //        node->set_type()
      }
      break;
    }
    default: {}
  }
}

void TypeChecker::VisitAssignmentStatement(ast::AssignmentStatement *node) {
  Visit(node->source());
}

void TypeChecker::VisitBlockStatement(ast::BlockStatement *node) {
  Scope *block_scope = OpenScope(Scope::Kind::Block);

  for (auto *stmt : node->statements()) {
    Visit(stmt);
  }

  CloseScope(block_scope);
}

void TypeChecker::VisitFile(ast::File *node) {
  Scope *file_scope = OpenScope(Scope::Kind::File);

  for (auto *decl : node->declarations()) {
    Visit(decl);
  }

  CloseScope(file_scope);
}

void TypeChecker::VisitVariableDeclaration(ast::VariableDeclaration *node) {
  if (scope()->LookupLocal(node->name())) {
    // Duplicate variable name
    return;
  }

  Visit(node->type_repr());

  // The type should be resolved now
  scope()->Declare(node->name(), node->type_repr()->type());
}

void TypeChecker::VisitFunctionDeclaration(ast::FunctionDeclaration *node) {
  Visit(node->function());
}

void TypeChecker::VisitStructDeclaration(ast::StructDeclaration *node) {}
void TypeChecker::VisitIdentifierExpression(ast::IdentifierExpression *node) {}
void TypeChecker::VisitCallExpression(ast::CallExpression *node) {}
void TypeChecker::VisitPointerTypeRepr(ast::PointerTypeRepr *node) {}
void TypeChecker::VisitLiteralExpression(ast::LiteralExpression *node) {}
void TypeChecker::VisitForStatement(ast::ForStatement *node) {}

void TypeChecker::VisitExpressionStatement(ast::ExpressionStatement *node) {
  Visit(node->expression());
}

void TypeChecker::VisitBadStatement(ast::BadStatement *node) {}
void TypeChecker::VisitStructTypeRepr(ast::StructTypeRepr *node) {}
void TypeChecker::VisitIfStatement(ast::IfStatement *node) {}

void TypeChecker::VisitDeclarationStatement(ast::DeclarationStatement *node) {
  Visit(node->declaration());
}

void TypeChecker::VisitArrayTypeRepr(ast::ArrayTypeRepr *node) {}

void TypeChecker::VisitBinaryExpression(ast::BinaryExpression *node) {}
void TypeChecker::VisitFunctionLiteralExpression(
    ast::FunctionLiteralExpression *node) {}
void TypeChecker::VisitReturnStatement(ast::ReturnStatement *node) {}
void TypeChecker::VisitFunctionTypeRepr(ast::FunctionTypeRepr *node) {}

}  // namespace tpl::sema
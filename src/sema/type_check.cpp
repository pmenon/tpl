#include "sema/type_check.h"

namespace tpl::sema {

TypeChecker::TypeChecker(util::Region &region, ErrorReporter &error_reporter)
    : region_(region), error_reporter_(error_reporter) {}

bool TypeChecker::Run(ast::AstNode *root) {
  Visit(root);
  return error_reporter().has_errors();
}

void TypeChecker::VisitBadExpression(ast::BadExpression *node) {

}

void TypeChecker::VisitUnaryExpression(ast::UnaryExpression *node) {
  Visit(node->expr());
}

void TypeChecker::VisitAssignmentStatement(ast::AssignmentStatement *node) {

}

void TypeChecker::VisitBlockStatement(ast::BlockStatement *node) {}

void TypeChecker::VisitFile(ast::File *node) {
  for (auto *decl : node->declarations()) {
    Visit(decl);
  }
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

void TypeChecker::VisitFunctionDeclaration(ast::FunctionDeclaration *node) {}
void TypeChecker::VisitStructDeclaration(ast::StructDeclaration *node) {}
void TypeChecker::VisitIdentifierExpression(ast::IdentifierExpression *node) {}
void TypeChecker::VisitCallExpression(ast::CallExpression *node) {}
void TypeChecker::VisitPointerTypeRepr(ast::PointerTypeRepr *node) {}
void TypeChecker::VisitLiteralExpression(ast::LiteralExpression *node) {}
void TypeChecker::VisitForStatement(ast::ForStatement *node) {}
void TypeChecker::VisitExpressionStatement(ast::ExpressionStatement *node) {}
void TypeChecker::VisitBadStatement(ast::BadStatement *node) {}
void TypeChecker::VisitStructTypeRepr(ast::StructTypeRepr *node) {}
void TypeChecker::VisitIfStatement(ast::IfStatement *node) {}
void TypeChecker::VisitDeclarationStatement(ast::DeclarationStatement *node) {}
void TypeChecker::VisitArrayTypeRepr(ast::ArrayTypeRepr *node) {}
void TypeChecker::VisitBinaryExpression(ast::BinaryExpression *node) {}
void TypeChecker::VisitFunctionLiteralExpression(
    ast::FunctionLiteralExpression *node) {}
void TypeChecker::VisitReturnStatement(ast::ReturnStatement *node) {}
void TypeChecker::VisitFunctionTypeRepr(ast::FunctionTypeRepr *node) {}

}  // namespace tpl::sema
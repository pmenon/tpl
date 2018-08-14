#include "parsing/type_checker.h"

namespace tpl::parsing {

bool TypeChecker::Run() {
  Visit(root_);
  return true;
}

void TypeChecker::VisitBadExpression(UNUSED ast::BadExpression *node) {
  TPL_ASSERT(false);
}

void TypeChecker::VisitUnaryExpression(ast::UnaryExpression *node) {}

void TypeChecker::VisitBlockStatement(ast::BlockStatement *node) {}

void TypeChecker::VisitFile(ast::File *node) {}

void TypeChecker::VisitVariableDeclaration(ast::VariableDeclaration *node) {}

void TypeChecker::VisitFunctionDeclaration(ast::FunctionDeclaration *node) {}

void TypeChecker::VisitStructType(ast::StructType *node) {}

void TypeChecker::VisitStructDeclaration(ast::StructDeclaration *node) {}

void TypeChecker::VisitIdentifierExpression(ast::IdentifierExpression *node) {}

void TypeChecker::VisitCallExpression(ast::CallExpression *node) {}

void TypeChecker::VisitLiteralExpression(ast::LiteralExpression *node) {}

void TypeChecker::VisitFunctionType(ast::FunctionType *node) {}

void TypeChecker::VisitForStatement(ast::ForStatement *node) {}

void TypeChecker::VisitArrayType(ast::ArrayType *node) {}

void TypeChecker::VisitBadStatement(UNUSED ast::BadStatement *node) {
  TPL_ASSERT(false);
}

void TypeChecker::VisitIfStatement(ast::IfStatement *node) {}

void TypeChecker::VisitExpressionStatement(ast::ExpressionStatement *node) {}

void TypeChecker::VisitReturnStatement(ast::ReturnStatement *node) {}

void TypeChecker::VisitPointerType(ast::PointerType *node) {}

void TypeChecker::VisitBinaryExpression(ast::BinaryExpression *node) {}

void TypeChecker::VisitFunctionLiteralExpression(
    ast::FunctionLiteralExpression *node) {}

void TypeChecker::VisitDeclarationStatement(ast::DeclarationStatement *node) {}

}  // namespace tpl::parsing
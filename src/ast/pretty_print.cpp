#include "ast/pretty_print.h"

namespace tpl {

void PrettyPrint::VisitFunctionDeclaration(FunctionDeclaration *node) {
  BeginVisit();

  EndVisit();
}

void PrettyPrint::VisitVariableDeclaration(VariableDeclaration *node) {
  BeginVisit();
  EndVisit();
}

void PrettyPrint::VisitStructDeclaration(StructDeclaration *node) {}

void PrettyPrint::VisitBlockStatement(BlockStatement *node) {
  for (auto *statement : node->statements()) {
    Visit(statement);
  }
}

void PrettyPrint::VisitDeclarationStatement(DeclarationStatement *node) {

}

void PrettyPrint::VisitExpressionStatement(ExpressionStatement *node) {
  Visit(node->expr());
}

void PrettyPrint::VisitIfStatement(IfStatement *node) {
  BeginVisit();

  PrintString("if ");
  Visit(node->cond());
  PrintString(" then ");
  Visit(node->then_stmt());

  if (node->else_stmt() != nullptr) {
    PrintString(" else ");
    Visit(node->else_stmt());
  }

  EndVisit();
}

void PrettyPrint::VisitReturnStatement(ReturnStatement *node) {}

void PrettyPrint::VisitCallExpression(CallExpression *node) {
  PrintString("call ");
  Visit(node->function());
  PrintString("(");

  bool first = true;
  for (auto *expr : node->arguments()) {
    if (!first) PrintString(", ");
    first = false;
    Visit(expr);
  }

  PrintString(")");
}

void PrettyPrint::VisitBinaryExpression(BinaryExpression *node) {
  BeginVisit();
  PrintToken(node->op());
  PrintString(" ");
  Visit(node->left());
  PrintString(" ");
  Visit(node->right());
  EndVisit();
}

void PrettyPrint::VisitFunctionLiteralExpression(
    FunctionLiteralExpression *node) {}

void PrettyPrint::VisitLiteralExpression(LiteralExpression *node) {
  switch (node->type()) {
    case LiteralExpression::Type::Nil: {
      PrintString("nil");
      break;
    }
    case LiteralExpression::Type::Boolean: {
      PrintString(node->bool_val() ? "'true'" : "'false'");
    }
    case LiteralExpression::Type::Number:
    case LiteralExpression::Type::String: {
      PrintString(node->raw_string());
      break;
    }
  }
}

void PrettyPrint::VisitUnaryExpression(UnaryExpression *node) {
  BeginVisit();
  PrintToken(node->op());
  result_.append(" ");
  Visit(node->expr());
  EndVisit();
}

void PrettyPrint::VisitVarExpression(VarExpression *node) {
  BeginVisit();
  PrintString(node->name());
  EndVisit();
}

}  // namespace tpl
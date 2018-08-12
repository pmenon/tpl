#include "ast/pretty_print.h"

namespace tpl {

void PrettyPrint::VisitFile(File *node) {
  BeginVisit();
  for (auto *decl : node->declarations()) {
    Visit(decl);
    NewLine();
  }
  EndVisit();
}

void PrettyPrint::VisitFunctionDeclaration(FunctionDeclaration *node) {
  PrintString("fun ");
  PrintString(node->name());
  PrintString(" ");
  Visit(node->type());
  PrintString(" ");
  Visit(node->function()->body());
}

void PrettyPrint::VisitVariableDeclaration(VariableDeclaration *node) {
  BeginVisit();
  PrintString("var ");
  PrintString(node->name());
  if (node->initial() != nullptr) {
    PrintString(" = ");
    Visit(node->initial());
  }
  EndVisit();
}

void PrettyPrint::VisitStructDeclaration(StructDeclaration *node) {
  BeginVisit();
  PrintString("struct ");
  PrintString(node->name());
  PrintString("{ ");
  bool first = true;
  for (const auto *field : node->type()->fields()) {
    if (!first) PrintString(",");
    first = false;
    PrintString(field->name());
    PrintString(":");
    Visit(field->type());
  }
  PrintString("}");
  EndVisit();
}

void PrettyPrint::VisitBlockStatement(BlockStatement *node) {
  BeginVisit();
  for (auto *statement : node->statements()) {
    Visit(statement);
    PrintString(";");
  }
  EndVisit();
}

void PrettyPrint::VisitDeclarationStatement(DeclarationStatement *node) {
  Visit(node->declaration());
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

void PrettyPrint::VisitReturnStatement(ReturnStatement *node) {
  BeginVisit();
  PrintString("return ");
  if (node->ret() != nullptr) {
    Visit(node->ret());
  }
  EndVisit();
}

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

void PrettyPrint::VisitStructType(StructType *node) {
  PrintString("struct {\n");
  for (const auto *field : node->fields()) {
    PrintString(field->name());
    PrintString(" : ");
    Visit(field->type());
  }
  PrintString("}");
}

void PrettyPrint::VisitPointerType(PointerType *node) {
  PrintString("*");
  Visit(node->pointee_type());
}

void PrettyPrint::VisitFunctionType(FunctionType *node) {
  PrintString("(");
  bool first = true;
  for (const auto *field : node->parameters()) {
    if (!first) PrintString(",");
    first = false;
    PrintString(field->name());
    PrintString(" : ");
    Visit(field->type());
  }
  PrintString(") -> ");
  Visit(node->return_type());
}

void PrettyPrint::VisitIdentifierType(IdentifierType *node) {
  PrintString(node->name());
}

void PrettyPrint::VisitArrayType(ArrayType *node) {
  PrintString("[");
  if (node->length() != nullptr) {
    Visit(node->length());
  }
  PrintString("]");
  Visit(node->element_type());
}

}  // namespace tpl
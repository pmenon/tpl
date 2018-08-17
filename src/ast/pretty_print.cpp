#include "ast/pretty_print.h"

namespace tpl::ast {

void PrettyPrint::VisitFile(File *node) {
  for (auto *decl : node->declarations()) {
    Visit(decl);
    NewLine();
  }
}

void PrettyPrint::VisitFunctionDeclaration(FunctionDeclaration *node) {
  PrintString("fun ");
  PrintString(node->name());
  PrintString(" ");
  Visit(node->type_repr());
  PrintString(" ");
  Visit(node->function());
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
  for (const auto *field : node->type_repr()->fields()) {
    if (!first) PrintString(",");
    first = false;
    PrintString(field->name());
    PrintString(":");
    Visit(field->type_repr());
  }
  PrintString("}");
  EndVisit();
}

void PrettyPrint::VisitAssignmentStatement(AssignmentStatement *node) {
  BeginVisit();
  PrintString("assign ");
  Visit(node->destination());
  PrintString(" = ");
  Visit(node->source());
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

void PrettyPrint::VisitForStatement(ForStatement *node) {
  BeginVisit();
  PrintString("for (");
  if (node->init() != nullptr) {
    Visit(node->init());
    PrintString(";");
  }
  if (node->cond() != nullptr) {
    Visit(node->cond());
    if (!node->is_while_like()) {
      PrintString(";");
    }
  }
  if (node->next() != nullptr) {
    Visit(node->next());
  }
  PrintString(")");
  Visit(node->body());
  EndVisit();
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
  BeginVisit();
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
  EndVisit();
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
    FunctionLiteralExpression *node) {
  Visit(node->body());
}

void PrettyPrint::VisitIdentifierExpression(IdentifierExpression *node) {
  PrintString("'");
  PrintString(node->name());
  PrintString("'");
}

void PrettyPrint::VisitLiteralExpression(LiteralExpression *node) {
  switch (node->literal_type()) {
    case LiteralExpression::Type::Nil: {
      PrintString("nil");
      break;
    }
    case LiteralExpression::Type::Boolean: {
      PrintString(node->bool_val() ? "'true'" : "'false'");
    }
    case LiteralExpression::Type::Int:
    case LiteralExpression::Type::Float:
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

void PrettyPrint::VisitStructTypeRepr(StructTypeRepr *node) {
  PrintString("struct {\n");
  for (const auto *field : node->fields()) {
    PrintString(field->name());
    PrintString(" : ");
    Visit(field->type_repr());
  }
  PrintString("}");
}

void PrettyPrint::VisitPointerTypeRepr(PointerTypeRepr *node) {
  PrintString("*");
  Visit(node->base());
}

void PrettyPrint::VisitFunctionTypeRepr(FunctionTypeRepr *node) {
  PrintString("(");
  bool first = true;
  for (const auto *field : node->parameters()) {
    if (!first) PrintString(",");
    first = false;
    PrintString(field->name());
    PrintString(" : ");
    Visit(field->type_repr());
  }
  PrintString(") -> ");
  Visit(node->return_type());
}

void PrettyPrint::VisitArrayTypeRepr(ArrayTypeRepr *node) {
  PrintString("[");
  if (node->length() != nullptr) {
    Visit(node->length());
  }
  PrintString("]");
  Visit(node->element_type());
}

void PrettyPrint::VisitBadStatement(BadStatement *node) {
  BeginVisit();
  PrintString("BAD STATEMENT @ ");
  PrintPosition(node->position());
  EndVisit();
}

void PrettyPrint::VisitBadExpression(BadExpression *node) {
  BeginVisit();
  PrintString("BAD EXPRESSION @ ");
  PrintPosition(node->position());
  EndVisit();
}

}  // namespace tpl::ast
#include "ast/pretty_print.h"

namespace tpl::ast {

void PrettyPrint::VisitFile(File *node) {
  for (auto *decl : node->declarations()) {
    Visit(decl);
    NewLine();
  }
}

void PrettyPrint::VisitFieldDecl(FieldDecl *node) {
  PrintIdentifier(node->name());
  PrintString(":");
  Visit(node->type_repr());
}

void PrettyPrint::VisitFunctionDecl(FunctionDecl *node) {
  PrintString("fun ");
  PrintIdentifier(node->name());
  PrintString(" ");
  Visit(node->type_repr());
  PrintString(" ");
  Visit(node->function());
}

void PrettyPrint::VisitVariableDecl(VariableDecl *node) {
  BeginVisit();
  PrintString("var ");
  PrintIdentifier(node->name());
  if (node->initial() != nullptr) {
    PrintString(" = ");
    Visit(node->initial());
  }
  EndVisit();
}

void PrettyPrint::VisitStructDecl(StructDecl *node) {
  BeginVisit();
  PrintString("struct ");
  PrintIdentifier(node->name());
  PrintString("{ ");
  bool first = true;
  for (const auto *field : node->type_repr()->fields()) {
    if (!first) PrintString(",");
    first = false;
    PrintIdentifier(field->name());
    PrintString(":");
    Visit(field->type_repr());
  }
  PrintString("}");
  EndVisit();
}

void PrettyPrint::VisitAssignmentStmt(AssignmentStmt *node) {
  BeginVisit();
  PrintString("assign ");
  Visit(node->destination());
  PrintString(" = ");
  Visit(node->source());
  EndVisit();
}

void PrettyPrint::VisitBlockStmt(BlockStmt *node) {
  BeginVisit();
  for (auto *statement : node->statements()) {
    Visit(statement);
    PrintString(";");
  }
  EndVisit();
}

void PrettyPrint::VisitDeclStmt(DeclStmt *node) { Visit(node->declaration()); }

void PrettyPrint::VisitExpressionStmt(ExpressionStmt *node) {
  Visit(node->expression());
}

void PrettyPrint::VisitForStmt(ForStmt *node) {
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

void PrettyPrint::VisitIfStmt(IfStmt *node) {
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

void PrettyPrint::VisitReturnStmt(ReturnStmt *node) {
  BeginVisit();
  PrintString("return ");
  if (node->ret() != nullptr) {
    Visit(node->ret());
  }
  EndVisit();
}

void PrettyPrint::VisitCallExpr(CallExpr *node) {
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

void PrettyPrint::VisitBinaryOpExpr(BinaryOpExpr *node) {
  BeginVisit();
  PrintToken(node->op());
  PrintString(" ");
  Visit(node->left());
  PrintString(" ");
  Visit(node->right());
  EndVisit();
}

void PrettyPrint::VisitFunctionLitExpr(FunctionLitExpr *node) {
  Visit(node->body());
}

void PrettyPrint::VisitIdentifierExpr(IdentifierExpr *node) {
  PrintString("'");
  PrintIdentifier(node->name());
  PrintString("'");
}

void PrettyPrint::VisitLitExpr(LitExpr *node) {
  switch (node->literal_kind()) {
    case LitExpr::LitKind::Nil: {
      PrintString("nil");
      break;
    }
    case LitExpr::LitKind::Boolean: {
      PrintString(node->bool_val() ? "'true'" : "'false'");
    }
    case LitExpr::LitKind::Int:
    case LitExpr::LitKind::Float:
    case LitExpr::LitKind::String: {
      PrintIdentifier(node->raw_string());
      break;
    }
  }
}

void PrettyPrint::VisitUnaryOpExpr(UnaryOpExpr *node) {
  BeginVisit();
  PrintToken(node->op());
  result_.append(" ");
  Visit(node->expr());
  EndVisit();
}

void PrettyPrint::VisitStructTypeRepr(StructTypeRepr *node) {
  PrintString("struct {\n");
  for (auto *field : node->fields()) {
    Visit(field);
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
  for (auto *field : node->parameters()) {
    if (!first) PrintString(",");
    first = false;
    Visit(field);
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

void PrettyPrint::VisitBadStmt(BadStmt *node) {
  BeginVisit();
  PrintString("BAD STATEMENT @ ");
  PrintPosition(node->position());
  EndVisit();
}

void PrettyPrint::VisitBadExpr(BadExpr *node) {
  BeginVisit();
  PrintString("BAD EXPRESSION @ ");
  PrintPosition(node->position());
  EndVisit();
}

}  // namespace tpl::ast
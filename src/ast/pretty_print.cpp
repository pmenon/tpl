#include "ast/pretty_print.h"

namespace tpl {

void PrettyPrint::VisitBlock(Block *node) {
  for (auto *statement : node->statements()) {
    Visit(statement);
  }
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

void PrettyPrint::VisitBinaryOperation(BinaryOperation *node) {
  BeginVisit();
  PrintToken(node->op());
  PrintString(" ");
  Visit(node->left());
  PrintString(" ");
  Visit(node->right());
  EndVisit();
}

void PrettyPrint::VisitLiteral(Literal *node) {
  switch (node->type()) {
    case Literal::Type::Nil: {
      PrintString("nil");
      break;
    }
    case Literal::Type::Boolean: {
      PrintString(node->bool_val() ? "'true'" : "'false'");
    }
    case Literal::Type::Number: {
      break;
    }
    case Literal::Type::String: {
      PrintString(node->raw_string());
      break;
    }
  }
}

void PrettyPrint::VisitUnaryOperation(UnaryOperation *node) {
  BeginVisit();
  PrintToken(node->op());
  result_.append(" ");
  Visit(node->expr());
  EndVisit();
}

void PrettyPrint::VisitVariable(Variable *node) {
  BeginVisit();
  PrintString("var ");
  PrintString(node->name());
  if (node->has_init()) {
    Visit(node->init());
  }
  EndVisit();
}

}  // namespace tpl
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
  result_.append("if ");
  Visit(node->cond());
  result_.append(" then ");
  Visit(node->then_stmt());
  if (node->else_stmt() != nullptr) {
    result_.append(" else ");
    Visit(node->else_stmt());
  }
  EndVisit();
}

void PrettyPrint::VisitBinaryOperation(BinaryOperation *node) {
  BeginVisit();
  MarkToken(node->op());
  result_.append(" ");
  Visit(node->left());
  result_.append(" ");
  Visit(node->right());
  EndVisit();
}

void PrettyPrint::VisitLiteral(Literal *node) {
  switch (node->type()) {
    case Literal::Type::Nil: {
      result_.append("nil");
      break;
    }
    case Literal::Type::Boolean: {
      result_.append(node->bool_val() ? "'true'" : "'false'");
    }
  }
}

void PrettyPrint::VisitUnaryOperation(UnaryOperation *node) {
  BeginVisit();
  MarkToken(node->op());
  result_.append(" ");
  Visit(node->expr());
  EndVisit();
}

void PrettyPrint::VisitVariable(Variable *node) {
  BeginVisit();
  result_.append("var ");
  if (node->has_init()) {
    Visit(node->init());
  }
  EndVisit();
}

}  // namespace tpl
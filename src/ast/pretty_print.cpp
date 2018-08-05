#include "ast/pretty_print.h"

namespace tpl {

void PrettyPrint::VisitBinaryOperation(BinaryOperation *node) {
  BeginVisit();
  MarkToken(node->op());
  result_.append(" ");
  Visit(node->left());
  result_.append(" ");
  Visit(node->right());
  EndVisit();
}

void PrettyPrint::VisitUnaryOperation(UnaryOperation *node) {
  BeginVisit();
  MarkToken(node->op());
  result_.append(" ");
  Visit(node->expr());
  EndVisit();
}

void PrettyPrint::VisitLiteral(Literal *node) {
  BeginVisit();
  result_.append("literal type:");
  int x = static_cast<std::underlying_type<Literal::Type>::type>(node->type());
  result_.append(std::to_string(x));
  EndVisit();
}

}  // namespace tpl
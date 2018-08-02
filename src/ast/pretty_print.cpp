#include "ast/pretty_print.h"

namespace tpl {

void PrettyPrint::VisitBinaryOperation(BinaryOperation *node) {
  printf("(+ \n");
}

void PrettyPrint::VisitUnaryOperation(UnaryOperation *node) {

}

}  // namespace tpl
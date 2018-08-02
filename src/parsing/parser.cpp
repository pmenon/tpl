#include "parsing/parser.h"

namespace tpl {

/*
 *
 */

Parser::Parser(Scanner &scanner, AstNodeFactory &node_factory)
    : scanner_(scanner), node_factory_(node_factory) {}

AstNode *Parser::Parse() {
  return node_factory_.NewBinaryOperation();
}

}  // namespace tpl
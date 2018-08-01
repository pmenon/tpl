#include "parser.h"

namespace tpl {

Parser::Parser(Scanner &scanner, AstNodeFactory &node_factory)
    : scanner_(scanner), node_factory_(node_factory) {}

std::unique_ptr<AstNode> Parser::Parse() { return std::unique_ptr<AstNode>(); }

}  // namespace tpl
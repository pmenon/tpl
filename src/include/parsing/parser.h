#pragma once

#include <memory>

#include "ast/ast.h"
#include "ast/ast_node_factory.h"
#include "parsing/scanner.h"

namespace tpl {

class Parser {
 public:
  Parser(Scanner &scanner, AstNodeFactory &node_factory);

  AstNode *Parse();

 private:
  // The main source scanner
  Scanner &scanner_;

  // A factory for all node types
  AstNodeFactory &node_factory_;
};

}  // namespace tpl
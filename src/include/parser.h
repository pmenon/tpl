#pragma once

#include <memory>

#include "ast.h"
#include "scanner.h"

namespace tpl {

class Parser {
 public:
  Parser(Scanner &scanner, AstNodeFactory &node_factory);

  std::unique_ptr<AstNode> Parse();

 private:
  // The main source scanner
  Scanner &scanner_;

  // A factory for all node types
  AstNodeFactory &node_factory_;
};

}  // namespace tpl
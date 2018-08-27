#pragma once

#include <string>

namespace tpl::ast {

class AstNode;

class AstDump {
 public:
  static void Dump(ast::AstNode *node);
};

}  // namespace tpl::ast
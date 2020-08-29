#pragma once

namespace tpl::ast {

class AstNode;

/**
 * Utility class to print an AST to standard output.
 */
class AstDump {
 public:
  static void Dump(AstNode *node);
};

}  // namespace tpl::ast

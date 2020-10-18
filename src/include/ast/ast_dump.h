#pragma once

#include <iosfwd>

namespace tpl::ast {

class AstNode;

/**
 * Utility class to print an AST to standard output.
 */
class AstDump {
 public:
  static void Dump(std::ostream &os, AstNode *node);
};

}  // namespace tpl::ast

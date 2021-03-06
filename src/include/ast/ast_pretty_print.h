#pragma once

#include <iosfwd>

namespace tpl::ast {

class AstNode;

/**
 * Utility class to pretty print an AST as a textual TPL program.
 */
class AstPrettyPrint {
 public:
  static void Dump(std::ostream &os, AstNode *node);
};

}  // namespace tpl::ast

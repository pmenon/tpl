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

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Simple accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  Scanner &scanner() { return scanner_; }
  AstNodeFactory &node_factory() { return node_factory_; }

 private:
  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Token logic
  ///
  //////////////////////////////////////////////////////////////////////////////

  Token::Type Next() { return scanner().Next(); }

  Token::Type peek() { return scanner().peek(); }

  void Consume(UNUSED Token::Type expected) {
    UNUSED Token::Type next = Next();
    TPL_ASSERT(next == expected);
  }

 private:
  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Parsing functions
  ///
  //////////////////////////////////////////////////////////////////////////////

  AstNode *ParseExpression();

  AstNode *ParseBinaryExpression(uint32_t min_prec);

  AstNode *ParseUnaryExpression();

  AstNode *ParsePrimaryExpression();

 private:
  // The source code scanner
  Scanner &scanner_;

  // A factory for all node types
  AstNodeFactory &node_factory_;
};

}  // namespace tpl
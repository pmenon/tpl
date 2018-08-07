#pragma once

#include "ast/ast.h"
#include "ast/ast_node_factory.h"
#include "ast/ast_value.h"
#include "parsing/scanner.h"

namespace tpl {

class Parser {
 public:
  Parser(Scanner &scanner, AstNodeFactory &node_factory,
         AstStringsContainer &strings_container);

  /*
   * Parse and generate an abstract syntax tree from the input TPL source code
   */
  AstNode *Parse();

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Simple accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  Scanner &scanner() { return scanner_; }

  AstNodeFactory &node_factory() { return node_factory_; }

  AstStringsContainer &strings_container() { return strings_container_; }

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

  void Expect(Token::Type expected) {
    Token::Type next = Next();
    if (next != expected) {
    }
  }

  // Get the current symbol as an AST string
  AstString *CurrentSymbol() {
    const std::string &literal = scanner().current_literal();
    return strings_container().GetAstString(literal);
  }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Parsing logic
  ///
  //////////////////////////////////////////////////////////////////////////////

  AstNode *ParseDeclaration();

  AstNode *ParseFunctionDeclaration();

  Statement *ParseBlock();

  Statement *ParseStatement();

  Expression *ParseExpression();

  Expression *ParseBinaryExpression(uint32_t min_prec);

  Expression *ParseUnaryExpression();

  Expression *ParsePrimaryExpression();

 private:
  // The source code scanner
  Scanner &scanner_;

  // A factory for all node types
  AstNodeFactory &node_factory_;

  // A factory for strings
  AstStringsContainer &strings_container_;
};

}  // namespace tpl
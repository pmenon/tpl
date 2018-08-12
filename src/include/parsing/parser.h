#pragma once

#include "ast/ast.h"
#include "ast/ast_node_factory.h"
#include "ast/ast_value.h"
#include "ast/scope.h"
#include "parsing/scanner.h"

namespace tpl::parsing {

class Parser {
 public:
  Parser(Scanner &scanner, ast::AstNodeFactory &node_factory,
         ast::AstStringsContainer &strings_container);

  /**
   * Parse and generate an abstract syntax tree from the input TPL source code
   */
  ast::AstNode *Parse();

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Simple accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  Scanner &scanner() { return scanner_; }

  ast::AstNodeFactory &node_factory() { return node_factory_; }

  util::Region &region() { return node_factory().region(); }

  ast::AstStringsContainer &strings_container() { return strings_container_; }

  ast::Scope *scope() { return scope_; }

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

  bool Matches(Token::Type expected) {
    if (peek() != expected) {
      return false;
    }

    Consume(expected);

    return true;
  }

  // Get the current symbol as an AST string
  ast::AstString *GetSymbol() {
    const std::string &literal = scanner().current_literal();
    return strings_container().GetAstString(literal);
  }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Parsing logic
  ///
  //////////////////////////////////////////////////////////////////////////////

  ast::Declaration *ParseDeclaration();

  ast::Declaration *ParseFunctionDeclaration();

  ast::Declaration *ParseStructDeclaration();

  ast::Declaration *ParseVariableDeclaration();

  ast::Statement *ParseStatement();

  ast::Statement *ParseExpressionStatement();

  ast::Statement *ParseBlockStatement();

  ast::Statement *ParseIfStatement();

  ast::Expression *ParseExpression();

  ast::Expression *ParseBinaryExpression(uint32_t min_prec);

  ast::Expression *ParseUnaryExpression();

  ast::Expression *ParsePrimaryExpression();

  ast::FunctionLiteralExpression *ParseFunctionLiteralExpression();

  ast::Type *ParseType();

  ast::IdentifierType *ParseIdentifierType();

  ast::FunctionType *ParseFunctionType();

  ast::PointerType *ParsePointerType();

  ast::ArrayType *ParseArrayType();

  ast::StructType *ParseStructType();

 private:
  // The source code scanner
  Scanner &scanner_;

  // A factory for all node types
  ast::AstNodeFactory &node_factory_;

  // A factory for strings
  ast::AstStringsContainer &strings_container_;

  // The current scope
  ast::Scope *scope_;
};

}  // namespace tpl::parsing
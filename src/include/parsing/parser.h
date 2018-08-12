#pragma once

#include "ast/ast.h"
#include "ast/ast_node_factory.h"
#include "ast/ast_value.h"
#include "ast/scope.h"
#include "parsing/scanner.h"
#include "util/region_containers.h"

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
  const ast::Scope *scope() const { return scope_; }

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
    TPL_ASSERT(next == expected &&
               "The next token doesn't match what was expected");
  }

  void Expect(Token::Type expected) {
    Token::Type next = Next();
    if (next != expected) {
      // An error happened, report it but move on ...
      ReportError("Unexpected token '%s' received when expected '%s'",
                  Token::String(next), Token::String(expected));
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
  /// Parsing productions
  ///
  //////////////////////////////////////////////////////////////////////////////

  ast::Declaration *ParseDeclaration();

  ast::Declaration *ParseFunctionDeclaration();

  ast::Declaration *ParseStructDeclaration();

  ast::Declaration *ParseVariableDeclaration();

  ast::Statement *ParseStatement();

  ast::Statement *ParseExpressionStatement();

  ast::Statement *ParseBlockStatement(ast::Scope *scope);

  ast::Statement *ParseIfStatement();

  ast::Expression *ParseExpression();

  ast::Expression *ParseBinaryExpression(uint32_t min_prec);

  ast::Expression *ParseUnaryExpression();

  ast::Expression *ParseCallExpression();

  ast::Expression *ParsePrimaryExpression();

  ast::Expression *ParseFunctionLiteralExpression();

  ast::Type *ParseType();

  ast::Type *ParseIdentifierType();

  ast::Type *ParseFunctionType();

  ast::Type *ParsePointerType();

  ast::Type *ParseArrayType();

  ast::Type *ParseStructType();

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Scopes
  ///
  //////////////////////////////////////////////////////////////////////////////

  class ScopeState {
   public:
    ScopeState(ast::Scope **scope_stack, ast::Scope *scope)
        : scope_stack_(scope_stack), outer_(*scope_stack) {
      *scope_stack = scope;
    }

    ~ScopeState() { *scope_stack_ = outer_; }

   private:
    ast::Scope **scope_stack_;
    ast::Scope *outer_;
  };

  ast::Scope *NewScope(ast::Scope::Type scope_type);

  ast::Scope *NewFunctionScope() {
    return NewScope(ast::Scope::Type::Function);
  }

  ast::Scope *NewBlockScope() { return NewScope(ast::Scope::Type::Block); }

  template <typename T>
  bool Resolve(T *node) const;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Error handling
  ///
  //////////////////////////////////////////////////////////////////////////////

  template <typename... Args>
  void ReportError(const char *fmt, const Args &... args);

 private:
  // The source code scanner
  Scanner &scanner_;

  // A factory for all node types
  ast::AstNodeFactory &node_factory_;

  // A factory for strings
  ast::AstStringsContainer &strings_container_;

  // The current scope
  ast::Scope *scope_;

  util::RegionVector<ast::AstNode *> unresolved_;
};

}  // namespace tpl::parsing
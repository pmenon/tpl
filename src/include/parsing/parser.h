#pragma once


#include <string>
#include <unordered_set>

#include "llvm/ADT/DenseMap.h"

#include "ast/ast.h"
#include "ast/ast_node_factory.h"
#include "ast/context.h"
#include "ast/identifier.h"
#include "parsing/parsing_context.h"
#include "parsing/scanner.h"
#include "sema/error_reporter.h"

namespace tpl::parsing {

class Parser {
 public:
  /// Build a parser instance using the given scanner and AST context
  /// \param scanner The scanner used to read input tokens
  /// \param context The context
  Parser(Scanner *scanner, ast::Context *context);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(Parser);

  /// Parse and generate an abstract syntax tree from the input TPL source code
  /// \return The generated AST
  ast::AstNode *Parse();

 private:
  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  Scanner *scanner() { return scanner_; }

  ast::Context *context() { return context_; }

  ast::AstNodeFactory *node_factory() { return node_factory_; }

  util::Region *region() { return context()->region(); }

  sema::ErrorReporter *error_reporter() { return error_reporter_; }

  // -------------------------------------------------------
  // Token logic
  // -------------------------------------------------------

  // Move to the next token in the stream
  Token::Type Next() { return scanner()->Next(); }

  // Peek at the next token in the stream
  Token::Type peek() { return scanner()->peek(); }

  void Consume(UNUSED Token::Type expected) {
    UNUSED Token::Type next = Next();
#ifndef NDEBUG
    if (next != expected) {
      error_reporter()->Report(scanner()->current_position(),
                               sema::ErrorMessages::kUnexpectedToken, next,
                               expected);
    }
#endif
  }

  // If the next token doesn't matched the given expected token, throw an error
  void Expect(Token::Type expected) {
    Token::Type next = Next();
    if (next != expected) {
      error_reporter()->Report(scanner()->current_position(),
                               sema::ErrorMessages::kUnexpectedToken, next,
                               expected);
    }
  }

  // If the next token matches the given expected token, consume it and return
  // true; otherwise, return false
  bool Matches(Token::Type expected) {
    if (peek() != expected) {
      return false;
    }

    Consume(expected);
    return true;
  }

  // Get the current symbol as an AST string
  ast::Identifier GetSymbol() {
    const std::string &literal = scanner()->current_literal();
    return context()->GetIdentifier(literal);
  }

  // In case of errors, sync up to any token in the list
  void Sync(const std::unordered_set<Token::Type> &s);

  // -------------------------------------------------------
  // Parsing productions
  // -------------------------------------------------------

  ast::Decl *ParseDecl(parsing::ParsingContext *pctx);

  ast::Decl *ParseFunctionDecl(parsing::ParsingContext *pctx);

  ast::Decl *ParseStructDecl(parsing::ParsingContext *pctx);

  ast::Decl *ParseVariableDecl(parsing::ParsingContext *pctx);

  ast::Stmt *ParseStmt(parsing::ParsingContext *pctx);

  ast::Stmt *ParseSimpleStmt(parsing::ParsingContext *pctx);

  ast::Stmt *ParseBlockStmt(parsing::ParsingContext *pctx);

  class ForHeader;

  ForHeader ParseForHeader(parsing::ParsingContext *pctx);

  ast::Stmt *ParseForStmt(parsing::ParsingContext *pctx);

  ast::Stmt *ParseIfStmt(parsing::ParsingContext *pctx);

  ast::Stmt *ParseReturnStmt(parsing::ParsingContext *pctx);

  ast::Expr *ParseExpr(parsing::ParsingContext *pctx);

  ast::Expr *ParseBinaryOpExpr(parsing::ParsingContext *pctx, u32 min_prec);

  ast::Expr *ParseUnaryOpExpr(parsing::ParsingContext *pctx);

  ast::Expr *ParseLeftHandSideExpression(parsing::ParsingContext *pctx);

  ast::Expr *ParsePrimaryExpr(parsing::ParsingContext *pctx);

  ast::Expr *ParseFunctionLitExpr(parsing::ParsingContext *pctx);

  ast::Expr *ParseType(parsing::ParsingContext *pctx);

  ast::Expr *ParseFunctionType(parsing::ParsingContext *pctx);

  ast::Expr *ParsePointerType(parsing::ParsingContext *pctx);

  ast::Expr *ParseArrayType(parsing::ParsingContext *pctx);

  ast::Expr *ParseStructType(parsing::ParsingContext *pctx);

  ast::Expr *ParseMapType(parsing::ParsingContext *pctx);

  ast::Attributes *ParseAttributes(parsing::ParsingContext *pctx);

 private:
  // The source code scanner
  Scanner *scanner_;

  // The context
  ast::Context *context_;

  // A factory for all node types
  ast::AstNodeFactory *node_factory_;

  // The error reporter
  sema::ErrorReporter *error_reporter_;
};

}  // namespace tpl::parsing

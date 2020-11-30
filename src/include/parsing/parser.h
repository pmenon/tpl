#pragma once

#include <string>
#include <unordered_set>

#include "ast/ast.h"
#include "ast/ast_node_factory.h"
#include "ast/context.h"
#include "ast/identifier.h"
#include "parsing/scanner.h"
#include "sema/error_reporter.h"

namespace tpl::parsing {

/**
 * Parser for TPL source.
 */
class Parser {
 public:
  /**
   * Build a parser instance using the given scanner and AST context.
   * @param scanner The scanner used to read input tokens.
   * @param context The context parsing occurs in. Mainly used for node creations.
   */
  Parser(Scanner *scanner, ast::Context *context);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(Parser);

  /**
   * Parse and generate an abstract syntax tree from the input TPL source code
   * @return The generated AST.
   */
  ast::AstNode *Parse();

 private:
  util::Region *Region() { return context_->GetRegion(); }

  // Move to the next token in the stream
  Token::Type Next() { return scanner_->Next(); }

  // Peek at the next token in the stream
  Token::Type Peek() const { return scanner_->Peek(); }

  // Consume one token. In debug mode, throw an error if the next token isn't
  // what was expected. In release mode, just consume the token without checking
  void Consume(UNUSED Token::Type expected) {
    UNUSED Token::Type next = Next();
#ifndef NDEBUG
    if (next != expected) {
      error_reporter_->Report(scanner_->CurrentPosition(), sema::ErrorMessages::kUnexpectedToken,
                              next, expected);
    }
#endif
  }

  // If the next token doesn't matched the given expected token, throw an error
  void Expect(Token::Type expected) {
    Token::Type next = Next();
    if (next != expected) {
      error_reporter_->Report(scanner_->CurrentPosition(), sema::ErrorMessages::kUnexpectedToken,
                              next, expected);
    }
  }

  // If the next token matches the given expected token, consume it and return
  // true; otherwise, return false
  bool Matches(Token::Type expected) {
    if (Peek() != expected) {
      return false;
    }

    Consume(expected);
    return true;
  }

  // Get the current symbol as an AST string
  ast::Identifier GetSymbol() {
    const std::string &literal = scanner_->CurrentLiteral();
    return context_->GetIdentifier(literal);
  }

  // In case of errors, sync up to any token in the list
  void Sync(const std::unordered_set<Token::Type> &s);

  // Cast the input node into an expression if it is one, otherwise report error
  ast::Expr *MakeExpr(ast::AstNode *node);

  // -------------------------------------------------------
  // Parsing productions
  // -------------------------------------------------------

  ast::Declaration *ParseDecl();

  ast::Declaration *ParseFunctionDecl();

  ast::Declaration *ParseStructDecl();

  ast::Declaration *ParseVariableDecl();

  ast::Stmt *ParseStmt();

  ast::Stmt *ParseSimpleStmt();

  ast::Stmt *ParseBlockStmt();

  class ForHeader;

  ForHeader ParseForHeader();

  ast::Stmt *ParseForStmt();

  ast::Stmt *ParseIfStmt();

  ast::Stmt *ParseReturnStmt();

  ast::Expr *ParseExpr();

  ast::Expr *ParseBinaryOpExpr(uint32_t min_prec);

  ast::Expr *ParseUnaryOpExpr();

  ast::Expr *ParsePrimaryExpr();

  ast::Expr *ParseOperand();

  ast::Expr *ParseFunctionLitExpr();

  ast::Expr *ParseType();

  ast::Expr *ParseFunctionType();

  ast::Expr *ParsePointerType();

  ast::Expr *ParseArrayType();

  ast::Expr *ParseStructType();

  ast::Expr *ParseMapType();

 private:
  // The source code scanner.
  Scanner *scanner_;
  // The context.
  ast::Context *context_;
  // A factory for all node types.
  ast::AstNodeFactory *node_factory_;
  // The error reporter.
  sema::ErrorReporter *error_reporter_;
};

}  // namespace tpl::parsing

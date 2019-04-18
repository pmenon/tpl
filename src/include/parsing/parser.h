#pragma once

#include <string>
#include <unordered_set>

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
  util::Region *region() { return context_->region(); }

  // Move to the next token in the stream
  Token::Type Next() { return scanner_->Next(); }

  // Peek at the next token in the stream
  Token::Type peek() const { return scanner_->peek(); }

  // Consume one token. In debug mode, throw an error if the next token isn't
  // what was expected. In release mode, just consume the token without checking
  void Consume(UNUSED Token::Type expected) {
    UNUSED Token::Type next = Next();
#ifndef NDEBUG
    if (next != expected) {
      error_reporter_->Report(scanner_->current_position(),
                              sema::ErrorMessages::kUnexpectedToken, next,
                              expected);
    }
#endif
  }

  // If the next token doesn't matched the given expected token, throw an error
  void Expect(Token::Type expected) {
    Token::Type next = Next();
    if (next != expected) {
      error_reporter_->Report(scanner_->current_position(),
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
    const std::string &literal = scanner_->current_literal();
    return context_->GetIdentifier(literal);
  }

  // In case of errors, sync up to any token in the list
  void Sync(const std::unordered_set<Token::Type> &s);

  // -------------------------------------------------------
  // Parsing productions
  // -------------------------------------------------------

  ast::Decl *ParseDecl(ParsingContext *pctx);

  ast::Decl *ParseFunctionDecl(ParsingContext *pctx);

  ast::Decl *ParseStructDecl(ParsingContext *pctx);

  ast::Decl *ParseVariableDecl(ParsingContext *pctx);

  ast::Stmt *ParseStmt(ParsingContext *pctx);

  ast::Stmt *ParseSimpleStmt(ParsingContext *pctx);

  ast::Stmt *ParseBlockStmt(ParsingContext *pctx);

  class ForHeader;

  ForHeader ParseForHeader(ParsingContext *pctx);

  ast::Stmt *ParseForStmt(ParsingContext *pctx);

  ast::Stmt *ParseIfStmt(ParsingContext *pctx);

  ast::Stmt *ParseReturnStmt(ParsingContext *pctx);

  ast::Expr *ParseExpr(ParsingContext *pctx);

  ast::Expr *ParseBinaryOpExpr(ParsingContext *pctx, u32 min_prec);

  ast::Expr *ParseUnaryOpExpr(ParsingContext *pctx);

  ast::Expr *ParsePrimaryExpr(ParsingContext *pctx);

  ast::Expr *ParseOperand(ParsingContext *pctx);

  ast::Expr *ParseFunctionLitExpr(ParsingContext *pctx);

  ast::Expr *ParseType(ParsingContext *pctx);

  ast::Expr *ParseFunctionType(ParsingContext *pctx);

  ast::Expr *ParsePointerType(ParsingContext *pctx);

  ast::Expr *ParseArrayType(ParsingContext *pctx);

  ast::Expr *ParseStructType(ParsingContext *pctx);

  ast::Expr *ParseMapType(ParsingContext *pctx);

  ast::Attributes *ParseAttributes(ParsingContext *pctx);

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

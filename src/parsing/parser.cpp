#include "parsing/parser.h"

#include "sema/error_reporter.h"

namespace tpl::parsing {

Parser::Parser(Scanner &scanner, ast::AstNodeFactory &node_factory,
               ast::AstStringsContainer &strings_container,
               sema::ErrorReporter &error_reporter)
    : scanner_(scanner),
      node_factory_(node_factory),
      strings_container_(strings_container),
      error_reporter_(error_reporter) {}

ast::AstNode *Parser::Parse() {
  util::RegionVector<ast::Declaration *> decls(region());

  const SourcePosition &start_pos = scanner().current_position();

  while (peek() != Token::Type::EOS) {
    decls.push_back(ParseDeclaration());
  }

  return node_factory().NewFile(start_pos, std::move(decls));
}

ast::Declaration *Parser::ParseDeclaration() {
  // At the top-level, we only allow structs and functions
  switch (peek()) {
    case Token::Type::STRUCT: {
      return ParseStructDeclaration();
    }
    case Token::Type::FUN: {
      return ParseFunctionDeclaration();
    }
    default: { return nullptr; }
  }
}

ast::Declaration *Parser::ParseFunctionDeclaration() {
  Expect(Token::Type::FUN);

  const SourcePosition &position = scanner().current_position();

  // The function name
  Expect(Token::Type::IDENTIFIER);
  ast::AstString *name = GetSymbol();

  // The function literal
  auto *fun =
      ParseFunctionLiteralExpression()->As<ast::FunctionLiteralExpression>();

  // Create declaration
  ast::FunctionDeclaration *decl =
      node_factory().NewFunctionDeclaration(position, name, fun);

  // Done
  return decl;
}

ast::Declaration *Parser::ParseStructDeclaration() {
  Expect(Token::Type::STRUCT);

  const SourcePosition &position = scanner().current_position();

  // The struct name
  Expect(Token::Type::IDENTIFIER);
  ast::AstString *name = GetSymbol();

  // The type
  auto *struct_type = ParseStructType()->As<ast::StructTypeRepr>();

  // The declaration object
  ast::StructDeclaration *decl =
      node_factory().NewStructDeclaration(position, name, struct_type);

  // Done
  return decl;
}

ast::Declaration *Parser::ParseVariableDeclaration() {
  // VariableDecl = 'var' Ident ':' Type [ '=' Expr ] ;

  Expect(Token::Type::VAR);

  const SourcePosition &position = scanner().current_position();

  // The name
  Expect(Token::Type::IDENTIFIER);
  ast::AstString *name = GetSymbol();

  // The type (if exists)
  ast::Expression *type = nullptr;

  if (Matches(Token::Type::COLON)) {
    type = ParseType();
  }

  // The initializer (if exists)
  ast::Expression *init = nullptr;

  if (Matches(Token::Type::EQUAL)) {
    init = ParseExpression();
  }

  // Create declaration object
  ast::VariableDeclaration *decl =
      node_factory().NewVariableDeclaration(position, name, type, init);

  // Done
  return decl;
}

ast::Statement *Parser::ParseStatement() {
  // Statement = Block | ExprStmt | ForStmt | IfStmt | ReturnStmt | SimpleStmt |
  // VariableDecl ;

  switch (peek()) {
    case Token::Type::LEFT_BRACE: {
      return ParseBlockStatement();
    }
    case Token::Type::FOR: {
      return ParseForStatement();
    }
    case Token::Type::IF: {
      return ParseIfStatement();
    }
    case Token::Type::RETURN: {
      return ParseReturnStatement();
    }
    case Token::Type::VAR: {
      ast::Declaration *var_decl = ParseVariableDeclaration();
      return node_factory().NewDeclarationStatement(var_decl);
    }
    default: { return ParseSimpleStatement(); }
  }
}

ast::Statement *Parser::ParseSimpleStatement() {
  // SimpleStmt = Assignment | ExpressionStatement
  ast::Expression *left = ParseExpression();

  if (Matches(Token::Type::EQUAL)) {
    const SourcePosition &pos = scanner().current_position();
    ast::Expression *right = ParseExpression();
    return node_factory().NewAssignmentStatement(pos, left, right);
  }

  return node_factory().NewExpressionStatement(left);
}

ast::Statement *Parser::ParseBlockStatement() {
  // BlockStmt = '{' { Stmt } '}' ;

  // Eat the left brace
  Expect(Token::Type::LEFT_BRACE);
  const SourcePosition &start_position = scanner().current_position();

  // Where we store all the statements in the block
  util::RegionVector<ast::Statement *> statements(region());

  // Loop while we don't see the right brace
  while (peek() != Token::Type::RIGHT_BRACE && peek() != Token::Type::EOS) {
    ast::Statement *stmt = ParseStatement();
    statements.emplace_back(stmt);
  }

  // Eat the right brace
  Expect(Token::Type::RIGHT_BRACE);
  const SourcePosition &end_position = scanner().current_position();

  return node_factory().NewBlockStatement(start_position, end_position,
                                          std::move(statements));
}

Parser::ForHeader Parser::ParseForHeader() {
  // ForStmt = 'for' '(' [ Condition | ForHeader ] ')' Block ;
  //
  // Condition = Expression ;
  //
  // ForHeader = [ Stmt ] ';' [ Condition ] ';' [ Stmt ]

  Expect(Token::Type::LEFT_PAREN);

  if (Matches(Token::Type::RIGHT_PAREN)) {
    // Infinite loop
    return {nullptr, nullptr, nullptr};
  }

  ast::Statement *init = nullptr;
  ast::Expression *cond = nullptr;
  ast::Statement *next = nullptr;

  init = ParseStatement();

  if (Matches(Token::Type::SEMI)) {
    // Regular for-loop
    if (!Matches(Token::Type::SEMI)) {
      cond = ParseExpression();
      Expect(Token::Type::SEMI);
    }
    if (!Matches(Token::Type::RIGHT_PAREN)) {
      next = ParseStatement();
      Expect(Token::Type::RIGHT_PAREN);
    }
  } else {
    // While-loop
    Expect(Token::Type::RIGHT_PAREN);
    if (auto *cond_stmt = init->SafeAs<ast::ExpressionStatement>()) {
      cond = cond_stmt->expression();
    } else if (auto *assign = init->SafeAs<ast::AssignmentStatement>()) {
      // Often, novice users coming from C/C++ may write 'for (x = b) {}'
      // wrongly assuming that assignments are expressions in TPL. We try to
      // catch that here.
      // TODO(pmenon): Fix me to print out expression string
      (void)assign;
      error_reporter().Report(sema::ErrorMessages::kAssignmentUsedAsValue, "",
                              "");
    }
    init = nullptr;
  }

  return {init, cond, next};
}

ast::Statement *Parser::ParseForStatement() {
  Expect(Token::Type::FOR);

  const SourcePosition &position = scanner().current_position();

  // Parse the header to get the initialization statement, loop condition and
  // next-value statement
  const auto &[init, cond, next] = ParseForHeader();

  // Now the loop body
  auto *body = ParseBlockStatement()->As<ast::BlockStatement>();

  // Done
  return node_factory().NewForStatement(position, init, cond, next, body);
}

ast::Statement *Parser::ParseIfStatement() {
  // IfStmt = 'if' '(' Expr ')' Block [ 'else' ( IfStmt | Block ) ];

  Expect(Token::Type::IF);

  const SourcePosition &position = scanner().current_position();

  // Handle condition
  Expect(Token::Type::LEFT_PAREN);
  ast::Expression *cond = ParseExpression();
  Expect(Token::Type::RIGHT_PAREN);

  // Handle 'then' statement
  auto *then_stmt = ParseBlockStatement()->As<ast::BlockStatement>();

  // Handle 'else' statement, if one exists
  ast::Statement *else_stmt = nullptr;
  if (Matches(Token::Type::ELSE)) {
    if (Matches(Token::Type::IF)) {
      else_stmt = ParseIfStatement();
    } else {
      else_stmt = ParseBlockStatement();
    }
  }

  return node_factory().NewIfStatement(position, cond, then_stmt, else_stmt);
}

ast::Statement *Parser::ParseReturnStatement() {
  Expect(Token::Type::RETURN);

  const SourcePosition &position = scanner().current_position();

  ast::Expression *ret = ParseExpression();

  return node_factory().NewReturnStatement(position, ret);
}

ast::Expression *Parser::ParseExpression() {
  return ParseBinaryExpression(Token::LowestPrecedence() + 1);
}

ast::Expression *Parser::ParseBinaryExpression(uint32_t min_prec) {
  TPL_ASSERT(min_prec > 0);

  ast::Expression *left = ParseUnaryExpression();

  for (uint32_t prec = Token::Precedence(peek()); prec > min_prec; prec--) {
    // It's possible that we reach a token that has lower precedence than the
    // minimum (e.g., EOS) so we check and early exit
    if (Token::Precedence(peek()) < min_prec) {
      break;
    }

    // Make sure to consume **all** tokens with the same precedence as the
    // current value before moving on to a lower precedence expression. This is
    // to handle cases like 1+2+3+4.
    while (Token::Precedence(peek()) == prec) {
      Token::Type op = Next();
      const SourcePosition &position = scanner().current_position();
      ast::Expression *right = ParseBinaryExpression(prec);
      left = node_factory().NewBinaryExpression(position, op, left, right);
    }
  }

  return left;
}

ast::Expression *Parser::ParseUnaryExpression() {
  // UnaryExpression ::
  //   '!' UnaryExpression
  //   '-' UnaryExpression
  //   '*' UnaryExpression
  //   '&' UnaryExpression

  switch (peek()) {
    case Token::Type::AMPERSAND:
    case Token::Type::BANG:
    case Token::Type::MINUS:
    case Token::Type::STAR: {
      Token::Type op = Next();
      const SourcePosition &position = scanner().current_position();
      ast::Expression *expr = ParseUnaryExpression();
      return node_factory().NewUnaryExpression(position, op, expr);
    }
    default:
      break;
  }

  return ParseCallExpression();
}

ast::Expression *Parser::ParseCallExpression() {
  // CallExpr ::
  //   PrimaryExpr '(' (Expr)* ')

  ast::Expression *result = ParsePrimaryExpression();

  if (Matches(Token::Type::LEFT_PAREN)) {
    // Parse arguments

    util::RegionVector<ast::Expression *> args(region());

    bool done = (peek() == Token::Type::RIGHT_PAREN);
    while (!done) {
      // Parse argument
      ast::Expression *arg = ParseExpression();
      args.push_back(arg);

      done = (peek() != Token::Type::COMMA);
      if (!done) {
        Next();
      }
    }

    Expect(Token::Type::RIGHT_PAREN);

    result = node_factory().NewCallExpression(result, std::move(args));
  }

  return result;
}

ast::Expression *Parser::ParsePrimaryExpression() {
  // PrimaryExpr =
  //   nil | 'true' | 'false' | Ident | Number | String | FunctionLiteral |
  //   '(' Expr ')'

  switch (peek()) {
    case Token::Type::NIL: {
      Consume(Token::Type::NIL);
      return node_factory().NewNilLiteral(scanner().current_position());
    }
    case Token::Type::TRUE: {
      Consume(Token::Type::TRUE);
      return node_factory().NewBoolLiteral(scanner().current_position(), true);
    }
    case Token::Type::FALSE: {
      Consume(Token::Type::FALSE);
      return node_factory().NewBoolLiteral(scanner().current_position(), false);
    }
    case Token::Type::IDENTIFIER: {
      Next();
      const SourcePosition &position = scanner().current_position();
      return node_factory().NewIdentifierExpression(position, GetSymbol());
    }
    case Token::Type::INTEGER: {
      Next();
      const SourcePosition &position = scanner().current_position();
      return node_factory().NewIntLiteral(position, GetSymbol());
    }
    case Token::Type::FLOAT: {
      Next();
      const SourcePosition &position = scanner().current_position();
      return node_factory().NewFloatLiteral(position, GetSymbol());
    }
    case Token::Type::STRING: {
      Next();
      const SourcePosition &position = scanner().current_position();
      return node_factory().NewStringLiteral(position, GetSymbol());
    }
    case Token::Type::FUN: {
      Next();
      return ParseFunctionLiteralExpression();
    }
    case Token::Type::LEFT_PAREN: {
      Consume(Token::Type::LEFT_PAREN);
      ast::Expression *expr = ParseExpression();
      Expect(Token::Type::RIGHT_PAREN);
      return expr;
    }
    default: { break; }
  }

  // Error
  // TODO(pmenon) Also advance to next statement
  error_reporter().Report(sema::ErrorMessages::kExpectingExpression);
  return node_factory().NewBadExpression(scanner().current_position());
}

ast::Expression *Parser::ParseFunctionLiteralExpression() {
  // FunctionLiteralExpr = Signature FunctionBody ;
  //
  // FunctionBody = Block ;

  // Parse the type
  auto *func_type = ParseFunctionType()->As<ast::FunctionTypeRepr>();

  // Parse the body
  auto *body = ParseBlockStatement()->As<ast::BlockStatement>();

  // Done
  return node_factory().NewFunctionLiteral(func_type, body);
}

ast::Expression *Parser::ParseType() {
  switch (peek()) {
    case Token::Type::IDENTIFIER: {
      Next();
      const SourcePosition &position = scanner().current_position();
      return node_factory().NewIdentifierExpression(position, GetSymbol());
    }
    case Token::Type::LEFT_PAREN: {
      return ParseFunctionType();
    }
    case Token::Type::STAR: {
      return ParsePointerType();
    }
    case Token::Type::LEFT_BRACKET: {
      return ParseArrayType();
    }
    case Token::Type::STRUCT: {
      return ParseStructType();
    }
    default: { break; }
  }

  // Error
  error_reporter().Report(sema::ErrorMessages::kExpectingType);

  return nullptr;
}

ast::Expression *Parser::ParseFunctionType() {
  // FuncType = '(' { Ident ':' Type } ')' '->' Type ;

  Consume(Token::Type::LEFT_PAREN);

  const SourcePosition &position = scanner().current_position();

  util::RegionVector<ast::Field *> params(region());

  while (true) {
    if (!Matches(Token::Type::IDENTIFIER)) {
      break;
    }

    const SourcePosition &field_position = scanner().current_position();

    // The parameter name
    ast::AstString *name = GetSymbol();

    // Prepare for parameter type by eating the colon (ew ...)
    Expect(Token::Type::COLON);

    // Parse the type
    ast::Expression *type = ParseType();

    // That's it
    params.push_back(node_factory().NewField(field_position, name, type));

    if (!Matches(Token::Type::COMMA)) {
      break;
    }
  }

  Expect(Token::Type::RIGHT_PAREN);
  Expect(Token::Type::ARROW);

  ast::Expression *ret = ParseType();

  return node_factory().NewFunctionType(position, std::move(params), ret);
}

ast::Expression *Parser::ParsePointerType() {
  // PointerTypeRepr = '*' Type ;

  Expect(Token::Type::STAR);

  const SourcePosition &position = scanner().current_position();

  ast::Expression *base = ParseType();

  return node_factory().NewPointerType(position, base);
}

ast::Expression *Parser::ParseArrayType() {
  // ArrayTypeRepr = '[' [ Length ] ']' Type ;
  // Length = Expr ;

  Consume(Token::Type::LEFT_BRACKET);

  const SourcePosition &position = scanner().current_position();

  // If the next token doesn't match a right bracket, it means we have a length
  ast::Expression *len = nullptr;
  if (!Matches(Token::Type::RIGHT_BRACKET)) {
    len = ParseExpression();
    Expect(Token::Type::RIGHT_BRACKET);
  }

  // Now the type
  ast::Expression *elem_type = ParseType();

  // Done
  return node_factory().NewArrayType(position, len, elem_type);
}

ast::Expression *Parser::ParseStructType() {
  // StructType = '{' { Ident ':' Type } '}' ;

  Consume(Token::Type::LEFT_BRACE);

  const SourcePosition &position = scanner().current_position();

  util::RegionVector<ast::Field *> fields(region());

  while (peek() != Token::Type::RIGHT_BRACE) {
    Expect(Token::Type::IDENTIFIER);

    const SourcePosition &field_position = scanner().current_position();

    // The parameter name
    ast::AstString *name = GetSymbol();

    // Prepare for parameter type by eating the colon (ew ...)
    Expect(Token::Type::COLON);

    // Parse the type
    ast::Expression *type = ParseType();

    // That's it
    fields.push_back(node_factory().NewField(field_position, name, type));
  }

  Consume(Token::Type::RIGHT_BRACE);

  return node_factory().NewStructType(position, std::move(fields));
}

}  // namespace tpl::parsing

#include <parsing/parser.h>

#include "parsing/parser.h"

namespace tpl::parsing {

Parser::Parser(Scanner &scanner, ast::AstNodeFactory &node_factory,
               ast::AstStringsContainer &strings_container)
    : scanner_(scanner),
      node_factory_(node_factory),
      strings_container_(strings_container),
      scope_(nullptr),
      unresolved_(region()) {
  scope_ = NewScope(ast::Scope::Type::File);
}

ast::AstNode *Parser::Parse() {
  util::RegionVector<ast::Declaration *> decls(region());

  while (peek() != Token::Type::EOS) {
    decls.push_back(ParseDeclaration());
  }

  // Try final resolution
  size_t j = 0;
  for (size_t i = 0; i < unresolved_.size(); i++) {
    ast::AstNode *node = unresolved_[i];
    if (auto *ident = node->SafeAs<ast::IdentifierExpression>()) {
      auto *decl = scope()->Lookup(ident->name());
      if (decl == nullptr) {
        unresolved_[j++] = unresolved_[i];
      }
    }
  }
  unresolved_.erase(unresolved_.begin() + j, unresolved_.end());

  return node_factory().NewFile(std::move(decls), std::move(unresolved_));
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
  Consume(Token::Type::FUN);

  // The function name
  Expect(Token::Type::IDENTIFIER);
  ast::AstString *name = GetSymbol();

  // The function literal
  auto *fun =
      ParseFunctionLiteralExpression()->As<ast::FunctionLiteralExpression>();

  // Create declaration
  ast::FunctionDeclaration *decl =
      node_factory().NewFunctionDeclaration(name, fun);

  // Declare function in current scope
  scope()->Declare(name, decl);

  // Done
  return decl;
}

ast::Declaration *Parser::ParseStructDeclaration() {
  Consume(Token::Type::STRUCT);

  // The struct name
  Expect(Token::Type::IDENTIFIER);
  ast::AstString *name = GetSymbol();

  // The type
  auto *struct_type = ParseStructType()->As<ast::StructType>();

  // The declaration object
  ast::StructDeclaration *decl =
      node_factory().NewStructDeclaration(name, struct_type);

  // Actually declare struct in scope
  scope()->Declare(name, decl);

  // Done
  return decl;
}

ast::Declaration *Parser::ParseVariableDeclaration() {
  // VariableDeclaration ::
  //   'var' Identifier ':' Type ('=' Expression)?

  Consume(Token::Type::VAR);

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
      node_factory().NewVariableDeclaration(name, type, init);

  // Declare variable in scope
  scope()->Declare(decl->name(), decl);

  // Done
  return decl;
}

ast::Statement *Parser::ParseStatement() {
  // Statement ::
  //   Block
  //   ExpressionStatement
  //   ForStatement
  //   IfStatement
  //   ReturnStatement
  //   VariableDeclaration

  switch (peek()) {
    case Token::Type::LEFT_BRACE: {
      return ParseBlockStatement(nullptr);
    }
    case Token::Type::FOR: {
      return ParseForStatement();
    }
    case Token::Type::IF: {
      return ParseIfStatement();
    }
    case Token::Type::RETURN: {
      Consume(Token::Type::RETURN);
      ast::Expression *ret = ParseExpression();
      return node_factory().NewReturnStatement(ret);
    }
    case Token::Type::VAR: {
      ast::Declaration *var_decl = ParseVariableDeclaration();
      return node_factory().NewDeclarationStatement(var_decl);
    }
    default: { return ParseExpressionStatement(); }
  }
}

ast::Statement *Parser::ParseExpressionStatement() {
  // ExpressionStatement ::
  //   Expression

  ast::Expression *expr = ParseExpression();
  return node_factory().NewExpressionStatement(expr);
}

ast::Statement *Parser::ParseBlockStatement(ast::Scope *scope) {
  // BlockStatement ::
  //   '{' (Statement)+ '}'

  if (scope == nullptr) {
    scope = NewBlockScope();
  }

  ScopeState scope_state(&scope_, scope);

  // Eat the left brace
  Expect(Token::Type::LEFT_BRACE);

  // Where we store all the statements in the block
  util::RegionVector<ast::Statement *> statements(region());

  // Loop while we don't see the right brace
  while (peek() != Token::Type::RIGHT_BRACE && peek() != Token::Type::EOS) {
    ast::Statement *stmt = ParseStatement();
    statements.emplace_back(stmt);
  }

  // Eat the right brace
  Expect(Token::Type::RIGHT_BRACE);

  return node_factory().NewBlockStatement(std::move(statements));
}

Parser::ForHeader Parser::ParseForHeader() {
  // ForStatement ::
  //   'for' '(' (Stmt)? ; (Expr)? ; (Stmt)? ')' '{' (StmtList)? '}'
  //   'for' '(' Expr ')' '{' (StmtList)? '}'
  //   'for' '(' ')' '{' (StmtList)? '}'

  Expect(Token::Type::LEFT_PAREN);

  if (Matches(Token::Type::RIGHT_PAREN)) {
    // Infinite loop
    return ForHeader(nullptr, nullptr, nullptr);
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
      cond = cond_stmt->expr();
    } else {
      ReportError("Loop condition is not expression, found '%s'",
                  init->kind_name());
    }
    init = nullptr;
  }

  return ForHeader(init, cond, next);
}

ast::Statement *Parser::ParseForStatement() {
  // ForStatement ::
  //   'for' '(' (Stmt)? ; (Expr)? ; (Stmt)? ')' '{' (StmtList)? '}'
  //   'for' '(' Expr ')' '{' (StmtList)? '}'
  //   'for' '(' ')' '{' (StmtList)? '}'

  Consume(Token::Type::FOR);

  ast::Scope *for_scope = NewBlockScope();

  ScopeState scope_state(&scope_, for_scope);

  ForHeader header = ParseForHeader();

  auto *body = ParseBlockStatement(for_scope)->As<ast::BlockStatement>();

  return node_factory().NewForStatement(header.init, header.cond, header.next,
                                        body);
}

ast::Statement *Parser::ParseIfStatement() {
  // IfStatement ::
  //   'if' '(' Expression ')' '{' Statement '}' ('else' '{' Statement '}')?

  Expect(Token::Type::IF);

  // Handle condition
  Expect(Token::Type::LEFT_PAREN);
  ast::Expression *cond = ParseExpression();
  Expect(Token::Type::RIGHT_PAREN);

  // Handle 'then' statement
  auto *then_stmt = ParseBlockStatement(nullptr)->As<ast::BlockStatement>();

  // Handle 'else' statement, if one exists
  ast::Statement *else_stmt = nullptr;
  if (Matches(Token::Type::ELSE)) {
    if (Matches(Token::Type::IF)) {
      else_stmt = ParseIfStatement();
    } else {
      else_stmt = ParseBlockStatement(nullptr);
    }
  }

  return node_factory().NewIfStatement(cond, then_stmt, else_stmt);
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
      ast::Expression *right = ParseBinaryExpression(prec);
      left = node_factory().NewBinaryExpression(op, left, right);
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
      ast::Expression *expr = ParseUnaryExpression();
      return node_factory().NewUnaryExpression(op, expr);
    }
    default:
      break;
  }

  return ParseCallExpression();
}

ast::Expression *Parser::ParseCallExpression() {
  // CallExpression ::
  //   PrimaryExpression '(' (Expression

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
  // PrimaryExpression ::
  //  nil
  //  'true'
  //  'false'
  //  Identifier
  //  Number
  //  String
  //  FunctionLiteral
  // '(' Expression ')'

  switch (peek()) {
    case Token::Type::NIL: {
      Consume(Token::Type::NIL);
      return node_factory().NewNilLiteral();
    }
    case Token::Type::TRUE: {
      Consume(Token::Type::TRUE);
      return node_factory().NewBoolLiteral(true);
    }
    case Token::Type::FALSE: {
      Consume(Token::Type::FALSE);
      return node_factory().NewBoolLiteral(false);
    }
    case Token::Type::IDENTIFIER: {
      Next();
      auto *ident = node_factory().NewIdentifierExpression(GetSymbol());
      Resolve(ident);
      return ident;
    }
    case Token::Type::NUMBER: {
      Next();
      return node_factory().NewNumLiteral(GetSymbol());
    }
    case Token::Type::STRING: {
      Next();
      return node_factory().NewStringLiteral(GetSymbol());
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
  ReportError("Unexpected token '%s' when attempting to parse primary",
              Token::String(peek()));
  return node_factory().NewBadExpression(scanner().current_raw_pos());
}

ast::Expression *Parser::ParseFunctionLiteralExpression() {
  // FunctionLiteralExpression
  //   FunctionType BlockStatement

  // Create a new scope for this function
  ast::Scope *func_scope = NewFunctionScope();

  // Parse the type
  auto *func_type = ParseFunctionType()->As<ast::FunctionType>();

  // Add formal parameters
  for (const auto *param : func_type->parameters()) {
    auto *param_decl = node_factory().NewVariableDeclaration(
        param->name(), param->type(), nullptr);
    func_scope->Declare(param->name(), param_decl);
  }

  // Parse the body
  auto *body = ParseBlockStatement(func_scope)->As<ast::BlockStatement>();

  // Done
  return node_factory().NewFunctionLiteral(func_type, body);
}

ast::Expression *Parser::ParseType() {
  switch (peek()) {
    case Token::Type::IDENTIFIER: {
      Next();
      auto *ident = node_factory().NewIdentifierExpression(GetSymbol());
      Resolve(ident);
      return ident;
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
  ReportError("Un-parsable type beginning with '%s'", Token::String(peek()));

  return nullptr;
}

ast::Expression *Parser::ParseFunctionType() {
  // FuncType ::
  //   '(' (Identifier ':' Type)? (',' Identifier ':' Type)* ')' '->' Type

  Consume(Token::Type::LEFT_PAREN);

  util::RegionVector<ast::Field *> params(region());

  while (true) {
    if (!Matches(Token::Type::IDENTIFIER)) {
      break;
    }

    // The parameter name
    ast::AstString *name = GetSymbol();

    // Prepare for parameter type by eating the colon (ew ...)
    Expect(Token::Type::COLON);

    // Parse the type
    ast::Expression *type = ParseType();

    // That's it
    params.push_back(node_factory().NewField(name, type));

    if (!Matches(Token::Type::COMMA)) {
      break;
    }
  }

  Expect(Token::Type::RIGHT_PAREN);
  Expect(Token::Type::ARROW);

  ast::Expression *ret = ParseType();

  return node_factory().NewFunctionType(std::move(params), ret);
}

ast::Expression *Parser::ParsePointerType() {
  // PointerType ::
  //   '*' Type

  Consume(Token::Type::STAR);
  ast::Expression *pointee = ParseType();
  return node_factory().NewPointerType(pointee);
}

ast::Expression *Parser::ParseArrayType() {
  // ArrayType ::
  //   '[' (Expr)? ']' Type

  Consume(Token::Type::LEFT_BRACKET);

  ast::Expression *len = nullptr;
  if (peek() != Token::Type::RIGHT_BRACKET) {
    len = ParseExpression();
  }

  Expect(Token::Type::RIGHT_BRACKET);

  ast::Expression *elem_type = ParseType();

  return node_factory().NewArrayType(len, elem_type);
}

ast::Expression *Parser::ParseStructType() {
  // StructType ::
  //   '{' (Identifier ':' Type)* '}'

  Consume(Token::Type::LEFT_BRACE);

  util::RegionVector<ast::Field *> fields(region());

  while (peek() != Token::Type::RIGHT_BRACE) {
    Expect(Token::Type::IDENTIFIER);
    ast::AstString *name = GetSymbol();

    Expect(Token::Type::COLON);

    ast::Expression *type = ParseType();

    fields.push_back(node_factory().NewField(name, type));
  }

  Consume(Token::Type::RIGHT_BRACE);

  return node_factory().NewStructType(std::move(fields));
}

ast::Scope *Parser::NewScope(ast::Scope::Type scope_type) {
  return new (region()) ast::Scope(region(), scope_, scope_type);
}

void Parser::Resolve(ast::Expression *node) {
  auto *identifier = node->SafeAs<ast::IdentifierExpression>();

  if (identifier == nullptr) {
    return;
  }

  ast::Declaration *decl = scope()->Lookup(identifier->name());

  if (decl == nullptr) {
    // No declaration found, unresolved
    unresolved_.push_back(identifier);
    return;
  }

  // Success, bind and notify
  identifier->BindTo(decl);
}

template <typename... Args>
void Parser::ReportError(UNUSED const char *fmt, UNUSED const Args &... args) {}

}  // namespace tpl::parsing
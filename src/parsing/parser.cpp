#include "parsing/parser.h"

namespace tpl {

Parser::Parser(Scanner &scanner, AstNodeFactory &node_factory,
               AstStringsContainer &strings_container)
    : scanner_(scanner),
      node_factory_(node_factory),
      strings_container_(strings_container),
      scope_(nullptr) {}

AstNode *Parser::Parse() {
  util::RegionVector<AstNode *> decls(region());

  while (peek() != Token::Type::EOS) {
    decls.push_back(ParseDeclaration());
  }

  return decls[0];
}

Declaration *Parser::ParseDeclaration() {
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

Declaration *Parser::ParseFunctionDeclaration() {
  Consume(Token::Type::FUN);

  // The function name
  Expect(Token::Type::IDENTIFIER);
  AstString *name = GetSymbol();

  // The function literal
  FunctionLiteralExpression *fun = ParseFunctionLiteralExpression();

  // Done
  return node_factory().NewFunctionDeclaration(name, fun);
}

Declaration *Parser::ParseStructDeclaration() {
  Consume(Token::Type::STRUCT);

  Expect(Token::Type::IDENTIFIER);
  AstString *name = GetSymbol();

  StructType *type = ParseStructType();

  return node_factory().NewStructDeclaration(name, type);
}

Declaration *Parser::ParseVariableDeclaration() {
  // VariableDeclaration ::
  //   'var' Identifier ':' Type ('=' Expression)?

  Consume(Token::Type::VAR);

  Expect(Token::Type::IDENTIFIER);
  AstString *name = GetSymbol();

  Type *type = nullptr;

  if (Matches(Token::Type::COLON)) {
    type = ParseType();
  }

  Expression *init = nullptr;

  if (Matches(Token::Type::EQUAL)) {
    init = ParseExpression();
  }

  return node_factory().NewVariableDeclaration(name, type, init);
}

Statement *Parser::ParseStatement() {
  // Statement ::
  //   Block
  //   ExpressionStatement
  //   ForStatement
  //   IfStatement
  //   ReturnStatement
  //   VariableDeclaration

  switch (peek()) {
    case Token::Type::LEFT_BRACE: {
      return ParseBlockStatement();
    }
    case Token::Type::IF: {
      return ParseIfStatement();
    }
    case Token::Type::RETURN: {
      Consume(Token::Type::RETURN);
      Expression *ret = ParseExpression();
      return node_factory().NewReturnStatement(ret);
    }
    case Token::Type::VAR: {
      Declaration *var_decl = ParseVariableDeclaration();
      return node_factory().NewDeclarationStatement(var_decl);
    }
    default: { return ParseExpressionStatement(); }
  }
}

Statement *Parser::ParseExpressionStatement() {
  // ExpressionStatement ::
  //   Expression

  Expression *expr = ParseExpression();
  return node_factory().NewExpressionStatement(expr);
}

Statement *Parser::ParseBlockStatement() {
  // BlockStatement ::
  //   '{' (Statement)+ '}'

  // Eat the left brace
  Expect(Token::Type::LEFT_BRACE);

  // Where we store all the statements in the block
  util::RegionVector<Statement *> statements(node_factory().region());

  // Loop while we don't see the right brace
  while (peek() != Token::Type::RIGHT_BRACE && peek() != Token::Type::EOS) {
    Statement *stmt = ParseStatement();
    statements.emplace_back(stmt);
  }

  // Eat the right brace
  Expect(Token::Type::RIGHT_BRACE);

  return node_factory().NewBlockStatement(std::move(statements));
}

Statement *Parser::ParseIfStatement() {
  // IfStatement ::
  //   'if' '(' Expression ')' '{' Statement '}' ('else' '{' Statement '}')?

  Expect(Token::Type::IF);

  // Handle condition
  Expect(Token::Type::LEFT_PAREN);
  Expression *cond = ParseExpression();
  Expect(Token::Type::RIGHT_PAREN);

  // Handle 'then' statement
  Statement *then_stmt = ParseBlockStatement();

  // Handle 'else' statement, if one exists
  Statement *else_stmt = nullptr;
  if (Matches(Token::Type::ELSE)) {
    else_stmt = ParseBlockStatement();
  }

  return node_factory().NewIfStatement(cond, then_stmt, else_stmt);
}

Expression *Parser::ParseExpression() {
  return ParseBinaryExpression(Token::LowestPrecedence() + 1);
}

Expression *Parser::ParseBinaryExpression(uint32_t min_prec) {
  TPL_ASSERT(min_prec > 0);

  Expression *left = ParseUnaryExpression();

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
      AstNode *right = ParseBinaryExpression(prec);
      left = node_factory().NewBinaryExpression(op, left, right);
    }
  }

  return left;
}

Expression *Parser::ParseUnaryExpression() {
  // UnaryExpression ::
  //   '!' UnaryExpression
  //   '-' UnaryExpression
  //   '*' UnaryExpression
  //   '&' UnaryExpression

  Token::Type type = peek();
  switch (type) {
    case Token::Type::AMPERSAND:
    case Token::Type::BANG:
    case Token::Type::MINUS:
    case Token::Type::STAR: {
      Token::Type op = Next();
      AstNode *expr = ParseUnaryExpression();
      return node_factory().NewUnaryExpression(op, expr);
    }
    default:
      break;
  }

  return ParsePrimaryExpression();
}

Expression *Parser::ParsePrimaryExpression() {
  // PrimaryExpression ::
  //  nil
  //  'true'
  //  'false'
  //  Identifier
  //  Number
  //  String
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
      AstString *name = GetSymbol();
      return node_factory().NewVarExpression(name);
    }
    case Token::Type::NUMBER: {
      Next();
      return node_factory().NewNumLiteral(GetSymbol());
    }
    case Token::Type::STRING: {
      Next();
      return node_factory().NewStringLiteral(GetSymbol());
    }
    case Token::Type::LEFT_PAREN: {
      Consume(Token::Type::LEFT_PAREN);
      Expression *expr = ParseExpression();
      Expect(Token::Type::RIGHT_PAREN);
      return expr;
    }
    default: {}
  }
}

FunctionLiteralExpression *Parser::ParseFunctionLiteralExpression() {
  // FunctionLiteralExpression
  //   FunctionType BlockStatement

  FunctionType *func_type = ParseFunctionType();

  BlockStatement *body = ParseBlockStatement()->As<BlockStatement>();

  return node_factory().NewFunctionLiteral(func_type, body);
}

Type *Parser::ParseType() {
  switch (peek()) {
    case Token::Type::IDENTIFIER: {
      return ParseIdentifierType();
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
}

IdentifierType *Parser::ParseIdentifierType() {
  // IdentifierType ::
  //   Identifier

  // Get the name
  Consume(Token::Type::IDENTIFIER);
  AstString *name = GetSymbol();

  // Create the type
  IdentifierType *type = node_factory().NewIdentifierType(name);

  // Try to resolve
  Declaration *decl = scope()->Lookup(name);
  if (decl != nullptr) {
    type->BindTo(decl);
  }

  return type;
}

FunctionType *Parser::ParseFunctionType() {
  // FuncType ::
  //   '(' (Identifier ':' Type)? (',' Identifier ':' Type)* ')' '->' Type

  Consume(Token::Type::LEFT_PAREN);

  util::RegionVector<Field *> params(region());

  while (true) {
    if (!Matches(Token::Type::IDENTIFIER)) {
      break;
    }

    // The parameter name
    AstString *name = GetSymbol();

    // Prepare for parameter type by eating the colon (ew ...)
    Expect(Token::Type::COLON);

    // Parse the type
    Type *type = ParseType();

    // That's it
    params.push_back(node_factory().NewField(name, type));

    if (!Matches(Token::Type::COMMA)) {
      break;
    }
  }

  Expect(Token::Type::RIGHT_PAREN);
  Expect(Token::Type::ARROW);

  Type *ret = ParseType();

  return node_factory().NewFunctionType(std::move(params), ret);
}

PointerType *Parser::ParsePointerType() {
  // PointerType ::
  //   '*' Type

  Consume(Token::Type::STAR);
  Type *pointee = ParseType();
  return node_factory().NewPointerType(pointee);
}

ArrayType *Parser::ParseArrayType() {
  // ArrayType ::
  //   '[' (Expr)? ']' Type

  Consume(Token::Type::LEFT_BRACKET);

  Expression *len = nullptr;
  if (peek() != Token::Type::RIGHT_BRACKET) {
    len = ParseExpression();
  }

  Expect(Token::Type::RIGHT_BRACKET);

  Type *elem_type = ParseType();

  return node_factory().NewArrayType(len, elem_type);
}

StructType *Parser::ParseStructType() {
  // StructType ::
  //   '{' (Identifier ':' Type)* '}'

  Consume(Token::Type::LEFT_BRACE);

  util::RegionVector<Field *> fields(region());
  while (peek() != Token::Type::RIGHT_BRACE) {
    Expect(Token::Type::IDENTIFIER);
    AstString *name = GetSymbol();

    Expect(Token::Type::COLON);

    Type *type = ParseType();

    fields.push_back(node_factory().NewField(name, type));
  }

  Consume(Token::Type::RIGHT_BRACE);

  return node_factory().NewStructType(std::move(fields));
}

}  // namespace tpl
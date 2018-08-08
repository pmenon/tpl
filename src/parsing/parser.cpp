#include "parsing/parser.h"

namespace tpl {

Parser::Parser(Scanner &scanner, AstNodeFactory &node_factory,
               AstStringsContainer &strings_container)
    : scanner_(scanner),
      node_factory_(node_factory),
      strings_container_(strings_container) {}

AstNode *Parser::Parse() { return ParseStatement(); }

AstNode *Parser::ParseDeclaration() { return ParseFunctionDeclaration(); }

AstNode *Parser::ParseFunctionDeclaration() {
  return ParseBlock();
}

Statement *Parser::ParseStatement() {
  switch (peek()) {
    case Token::Type::LEFT_BRACE: {
      return ParseBlock();
    }
    case Token::Type::IF: {
      return ParseIfStatement();
    }
    default: {
      return ParseExpressionStatement();
    }
  }
}

Statement *Parser::ParseExpressionStatement() {
  Expression *expr = ParseExpression();
  return node_factory().NewExpressionStatement(expr);
}

Statement *Parser::ParseBlock() {
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

  return node_factory().NewBlock(std::move(statements));
}

Statement *Parser::ParseIfStatement() {
  Expect(Token::Type::IF);

  // Handle condition
  Expect(Token::Type::LEFT_PAREN);
  Expression *cond = ParseExpression();
  Expect(Token::Type::RIGHT_PAREN);

  // Handle 'then' statement
  Statement *then_stmt = ParseBlock();

  // Handle 'else' statement, if one exists
  Statement *else_stmt = nullptr;
  if (Matches(Token::Type::ELSE)) {
    else_stmt = ParseBlock();
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
      left = node_factory().NewBinaryOperation(op, left, right);
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
      return node_factory().NewUnaryOperation(op, expr);
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
      return node_factory().NewVariable(name, nullptr);
    }
    case Token::Type::NUMBER: {
      // TODO: Fix me
      Consume(Token::Type::NUMBER);
      double val = std::stod(scanner().current_literal());
      return node_factory().NewNumLiteral(val);
    }
    case Token::Type::STRING: {
      Next();
      auto *str = node_factory().NewStringLiteral(GetSymbol());
      return str;
    }
    case Token::Type::LEFT_PAREN: {
      Consume(Token::Type::LEFT_PAREN);
      Expression *expr = ParseExpression();
      Consume(Token::Type::RIGHT_PAREN);
      return expr;
    }
    default: {}
  }
}

}  // namespace tpl
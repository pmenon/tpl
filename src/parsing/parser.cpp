#include "parsing/parser.h"

namespace tpl {

Parser::Parser(Scanner &scanner, AstNodeFactory &node_factory)
    : scanner_(scanner), node_factory_(node_factory) {}

AstNode *Parser::Parse() { return ParseExpression(); }

AstNode *Parser::ParseExpression() {
  return ParseBinaryExpression(Token::LowestPrecedence() + 1);
}

AstNode *Parser::ParseBinaryExpression(uint32_t min_prec) {
  TPL_ASSERT(min_prec > 0);

  AstNode *left = ParseUnaryExpression();

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

AstNode *Parser::ParseUnaryExpression() {
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
      Token::Type op = type;
      Next();
      AstNode *expr = ParseUnaryExpression();
      return node_factory().NewUnaryOperation(op, expr);
    }
    default:
      break;
  }

  return ParsePrimaryExpression();
}

AstNode *Parser::ParsePrimaryExpression() {
  // PrimaryExpression ::
  //  nil
  //  'true'
  //  'false'
  //  Identifier
  //  Number
  //  String
  // '(' Expression ')'

  Token::Type token = peek();
  switch (token) {
    case Token::Type::NIL: {
      Consume(Token::Type::NIL);
      return node_factory().NewLiteral(Literal::Type::Nil);
    }
    case Token::Type::TRUE: {
      Consume(Token::Type::TRUE);
      return node_factory().NewLiteral(Literal::Type::True);
    }
    case Token::Type::FALSE: {
      Consume(Token::Type::FALSE);
      return node_factory().NewLiteral(Literal::Type::False);
    }
    case Token::Type::IDENTIFIER: {
      Consume(Token::Type::IDENTIFIER);
      return node_factory().NewLiteral(Literal::Type::Identifier);
    }
    case Token::Type::NUMBER: {
      Consume(Token::Type::NUMBER);
      return node_factory().NewLiteral(Literal::Type::Number);
    }
    case Token::Type::STRING: {
      Consume(Token::Type::STRING);
      return node_factory().NewLiteral(Literal::Type::String);
    }
    case Token::Type::LEFT_PAREN: {
      Consume(Token::Type::LEFT_PAREN);
      AstNode *expr = ParseExpression();
      Consume(Token::Type::RIGHT_PAREN);
      return expr;
    }
    default: {}
  }
}

}  // namespace tpl
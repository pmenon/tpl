#pragma once

#include <cstdint>

namespace tpl {

enum class TokenType : uint32_t {
  // Single-character tokens
  LEFT_PAREN, RIGHT_PAREN,
  LEFT_BRACE, RIGHT_BRACE,
  LEFT_BRACKET, RIGHT_BRACKET,
  AMPERSAND, BAND, COLON, COMMA, DOT,
  EQUAL, MINUS, PLUS, SLASH, STAR,

  // Two-character tokens
  BANG_EQUAL, EQUAL_EQUAL,
  GREATER, GREATER_EQUAL,
  LESS, LESS_EQUAL,

  // Literals
  IDENTIFIER, NUMBER, STRING,

  // Keywords
  AND, ELSE, FALSE, FOR, FUN, IF,
  NIL, OR, RETURN, TRUE, VAR, WHILE,

  ERROR, EOF_
};

class Token {

};

}  // namespace tpl
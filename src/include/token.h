#pragma once

#include <stdint.h>

namespace tpl {

#define TOKENS(T, K)             \
  /* Single-character tokens */  \
  T(LEFT_PAREN, "(")             \
  T(RIGHT_PAREN, ")")            \
  T(LEFT_BRACE, "{")             \
  T(RIGHT_BRACE, "}")            \
  T(LEFT_BRACKET, "[")           \
  T(RIGHT_BRACKET, "]")          \
  T(AMPERSAND, "&")              \
  T(BANG, "!")                   \
  T(COLON, ":")                  \
  T(COMMA, ",")                  \
  T(DOT, ".")                    \
  T(EQUAL, "=")                  \
  T(MINUS, "-")                  \
  T(PERCENT, "%")                \
  T(PLUS, "+")                   \
  T(SLASH, "/")                  \
  T(STAR, "*")                   \
                                 \
  /* Two-character tokens*/      \
  T(BANG_EQUAL, "!=")            \
  T(EQUAL_EQUAL, "==")           \
  T(GREATER, ">")                \
  T(GREATER_EQUAL, ">=")         \
  T(LESS, "<")                   \
  T(LESS_EQUAL, "<=")            \
                                 \
  /* Identifiers and literals */ \
  T(IDENTIFIER, "id")            \
  T(NUMBER, "num")               \
  T(STRING, "str")               \
                                 \
  /* Keywords */                 \
  K(AND, "and")                  \
  K(ELSE, "else")                \
  K(FALSE, "false")              \
  K(FOR, "for")                  \
  K(FUN, "fun")                  \
  K(IF, "if")                    \
  K(NIL, "nil")                  \
  K(OR, "or")                    \
  K(RETURN, "return")            \
  K(STRUCT, "struct")            \
  K(TRUE, "true")                \
  K(VAR, "var")                  \
  K(WHILE, "while")              \
                                 \
  /* Internal */                 \
  T(UNINIITIALIZED, nullptr)     \
  T(WHITESPACE, nullptr)         \
  T(ERROR, "error")              \
                                 \
  /* End of stream */            \
  T(EOS, "eos")

class Token {
 public:
#define T(typ, str) typ,
  enum Type { TOKENS(T, T) NUM_TOKENS };
#undef T

  static const char *Name(Type type) { return name_[type]; }

  static const char *String(Type type) { return string_[type]; }

  static constexpr uint32_t NumTokens() { return Type::NUM_TOKENS; }

 private:
  static const char *name_[Type::NUM_TOKENS];
  static const char *string_[Type::NUM_TOKENS];
};

}  // namespace tpl
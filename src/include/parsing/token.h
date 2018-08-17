#pragma once

#include <cstdint>

namespace tpl::parsing {

/*
 *
 */
#define TOKENS(T, K)                               \
  /* Punctuations */                               \
  T(LEFT_PAREN, "(", 0)                            \
  T(RIGHT_PAREN, ")", 0)                           \
  T(LEFT_BRACE, "{", 0)                            \
  T(RIGHT_BRACE, "}", 0)                           \
  T(LEFT_BRACKET, "[", 0)                          \
  T(RIGHT_BRACKET, "]", 0)                         \
  T(AMPERSAND, "&", 0)                             \
  T(ARROW, "->", 0)                                \
  T(BANG, "!", 0)                                  \
  T(COLON, ":", 0)                                 \
  T(DOT, ".", 0)                                   \
  T(SEMI, ";", 0)                                  \
                                                   \
  /* Assignment */                                 \
  T(EQUAL, "=", 0)                                 \
                                                   \
  /* Binary operators, sorted by precedence */     \
  T(COMMA, ",", 1)                                 \
  K(OR, "or", 3)                                   \
  K(AND, "and", 4)                                 \
  T(PLUS, "+", 7)                                  \
  T(MINUS, "-", 7)                                 \
  T(STAR, "*", 8)                                  \
  T(SLASH, "/", 8)                                 \
  T(PERCENT, "%", 8)                               \
                                                   \
  /* Comparison operators, sorted by precedence */ \
  T(BANG_EQUAL, "!=", 5)                           \
  T(EQUAL_EQUAL, "==", 5)                          \
  T(GREATER, ">", 6)                               \
  T(GREATER_EQUAL, ">=", 6)                        \
  T(LESS, "<", 6)                                  \
  T(LESS_EQUAL, "<=", 6)                           \
                                                   \
  /* Identifiers and literals */                   \
  T(IDENTIFIER, nullptr, 0)                        \
  T(INTEGER, "num", 0)                             \
  T(FLOAT, "float", 0)                             \
  T(STRING, "str", 0)                              \
                                                   \
  /* Non-binary operator keywords */               \
  K(ELSE, "else", 0)                               \
  K(FALSE, "false", 0)                             \
  K(FOR, "for", 0)                                 \
  K(FUN, "fun", 0)                                 \
  K(IF, "if", 0)                                   \
  K(NIL, "nil", 0)                                 \
  K(RETURN, "return", 0)                           \
  K(STRUCT, "struct", 0)                           \
  K(TRUE, "true", 0)                               \
  K(VAR, "var", 0)                                 \
                                                   \
  /* Internal */                                   \
  T(UNINIITIALIZED, nullptr, 0)                    \
  T(WHITESPACE, nullptr, 0)                        \
  T(ERROR, "error", 0)                             \
                                                   \
  /* End of stream */                              \
  T(EOS, "eos", 0)

class Token {
 public:
#define T(name, str, precedence) name,
  enum Type { TOKENS(T, T) NUM_TOKENS };
#undef T

  static const char *Name(Type type) { return name_[type]; }

  static const char *String(Type type) { return string_[type]; }

  static constexpr const uint32_t LowestPrecedence() { return 0; }

  static const uint32_t Precedence(Type type) { return precedence_[type]; }

  static constexpr uint32_t NumTokens() { return Type::NUM_TOKENS; }

 private:
  static const char *name_[Type::NUM_TOKENS];
  static const char *string_[Type::NUM_TOKENS];
  static const uint32_t precedence_[Type::NUM_TOKENS];
};

}  // namespace tpl::parsing
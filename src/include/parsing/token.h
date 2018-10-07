#pragma once

#include <cstdint>

#include "util/common.h"

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
  T(ARROW, "->", 0)                                \
  T(BANG, "!", 0)                                  \
  T(BIT_NOT, "~", 0)                               \
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
  T(BIT_OR, "|", 6)                                \
  T(BIT_XOR, "^", 7)                               \
  T(AMPERSAND, "&", 8)                             \
  T(PLUS, "+", 9)                                  \
  T(MINUS, "-", 9)                                 \
  T(STAR, "*", 10)                                 \
  T(SLASH, "/", 10)                                \
  T(PERCENT, "%", 10)                              \
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
  T(IDENTIFIER, "[ident]", 0)                      \
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
  enum class Type : u8 {
    TOKENS(T, T)
#undef T
#define T(name, str, precedence) +1
        Last = -1 TOKENS(T, T)
#undef T
  };

  static const u32 kTokenCount = static_cast<u32>(Type::Last) + 1;

  // Get the name of a given token type
  static const char *Name(Type type) { return name_[static_cast<u32>(type)]; }

  // Get the stringified version of a given token type
  static const char *String(Type type) {
    return string_[static_cast<u32>(type)];
  }

  // Get the precedence of a given token
  static const u32 Precedence(Type type) {
    return precedence_[static_cast<u32>(type)];
  }

  // Get the lowest operator precedence we support
  static constexpr const u32 LowestPrecedence() { return 0; }

  // Is the given token a comparison operator?
  static bool IsCompareOp(Type op) {
    return (static_cast<u8>(Type::BANG_EQUAL) <= static_cast<u8>(op) &&
            static_cast<u8>(op) <= static_cast<u8>(Type::LESS_EQUAL));
  }

 private:
  static const char *name_[];
  static const char *string_[];
  static const u32 precedence_[];
};

}  // namespace tpl::parsing

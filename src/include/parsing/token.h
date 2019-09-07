#pragma once

#include <cstdint>

#include "common/common.h"

namespace tpl::parsing {

/*
 * List of all tokens + keywords that accepts two callback functions. T() is
 * invoked for all symbol tokens, and K() is invoked for all keyword tokens.
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
  T(AT, "@", 0)                                    \
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
  T(BUILTIN_IDENTIFIER, "@[ident]", 0)             \
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
  K(IN, "in", 0)                                   \
  K(MAP, "map", 0)                                 \
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
  enum class Type : uint8_t {
#define T(name, str, precedence) name,
    TOKENS(T, T)
#undef T
#define T(name, str, precedence) +1
        Last = -1 TOKENS(T, T)
#undef T
  };

  static const uint32_t kTokenCount = static_cast<uint32_t>(Type::Last) + 1;

  // Get the name of a given token type
  static const char *GetName(Type type) { return kTokenNames[static_cast<uint32_t>(type)]; }

  // Get the stringified version of a given token type
  static const char *GetString(Type type) { return kTokenStrings[static_cast<uint32_t>(type)]; }

  // Get the precedence of a given token
  static uint32_t GetPrecedence(Type type) { return kTokenPrecedence[static_cast<uint32_t>(type)]; }

  // Get the lowest operator precedence we support
  static uint32_t LowestPrecedence() { return 0; }

  // Is the given token a comparison operator?
  static bool IsCompareOp(Type op) {
    return (static_cast<uint8_t>(Type::BANG_EQUAL) <= static_cast<uint8_t>(op) &&
            static_cast<uint8_t>(op) <= static_cast<uint8_t>(Type::LESS_EQUAL));
  }

  // If the given token either '==' or '!='
  static bool IsEqualityOp(Type op) { return op == Type::BANG_EQUAL || op == Type::EQUAL_EQUAL; }

  static bool IsCallOrMemberOrIndex(Type op) {
    return (op == Type::LEFT_PAREN || op == Type::DOT || op == Type::LEFT_BRACKET);
  }

 private:
  static const char *kTokenNames[];
  static const char *kTokenStrings[];
  static const uint32_t kTokenPrecedence[];
};

}  // namespace tpl::parsing

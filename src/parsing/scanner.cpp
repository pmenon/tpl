#include "parsing/scanner.h"

#include <parsing/scanner.h>
#include <cassert>
#include <stdexcept>

namespace tpl {

Scanner::Scanner(const char *source, uint64_t source_len)
    : source_(source), source_len_(source_len), offset_(0) {
  // Setup current token information
  curr_.type = Token::Type::UNINIITIALIZED;
  curr_.offset = 0;
  curr_.pos.line = 0;
  curr_.pos.column = 0;

  next_.type = Token::Type::UNINIITIALIZED;
  next_.offset = 0;
  next_.pos.line = 0;
  next_.pos.column = 0;

  // Advance character iterator to the first slot
  Advance();
  c0_pos_.line = 1;
  c0_pos_.column = 1;

  // Find the first token
  Scan();
}

Token::Type Scanner::Next() {
  curr_ = next_;
  Scan();
  return curr_.type;
}

void Scanner::Scan() {
  // Re-init the next token
  next_.literal.clear();

  // The token
  Token::Type type;

  do {
    // Setup current token positions
    next_.pos.line = c0_pos_.line;
    next_.pos.column = c0_pos_.column;
    next_.offset = offset_;

    switch (c0_) {
      case '{': {
        Advance();
        type = Token::Type::LEFT_BRACE;
        break;
      }
      case '}': {
        Advance();
        type = Token::Type::RIGHT_BRACE;
        break;
      }
      case '(': {
        Advance();
        type = Token::Type::LEFT_PAREN;
        break;
      }
      case ')': {
        Advance();
        type = Token::Type::RIGHT_PAREN;
        break;
      }
      case '[': {
        Advance();
        type = Token::Type::LEFT_BRACKET;
        break;
      }
      case ']': {
        Advance();
        type = Token::Type::RIGHT_BRACKET;
        break;
      }
      case '&': {
        Advance();
        type = Token::Type::AMPERSAND;
        break;
      }
      case '!': {
        Advance();
        if (Matches('=')) {
          type = Token::Type::BANG_EQUAL;
        } else {
          type = Token::Type::BANG;
        }
        break;
      }
      case ':': {
        Advance();
        type = Token::Type::COLON;
        break;
      }
      case ',': {
        Advance();
        type = Token::Type::COMMA;
        break;
      }
      case '.': {
        Advance();
        type = Token::Type::DOT;
        break;
      }
      case '=': {
        Advance();
        if (Matches('=')) {
          type = Token::Type::EQUAL_EQUAL;
        } else {
          type = Token::Type::EQUAL;
        }
        break;
      }
      case '>': {
        Advance();
        if (Matches('=')) {
          Advance();
          type = Token::Type::GREATER_EQUAL;
        } else {
          type = Token::Type::GREATER;
        }
        break;
      }
      case '<': {
        Advance();
        if (Matches('=')) {
          Advance();
          type = Token::Type::LESS_EQUAL;
        } else {
          type = Token::Type::LESS;
        }
        break;
      }
      case '-': {
        Advance();
        type = Token::Type::MINUS;
        break;
      }
      case '%': {
        Advance();
        type = Token::Type::PERCENT;
        break;
      }
      case '+': {
        Advance();
        type = Token::Type::PLUS;
        break;
      }
      case '/': {
        Advance();
        if (Matches('/')) {
          SkipLineComment();
          type = Token::Type::WHITESPACE;
        } else {
          type = Token::Type::SLASH;
        }
        break;
      }
      case '*': {
        Advance();
        type = Token::Type::STAR;
        break;
      }
      case '"': {
        Advance();
        type = ScanString();
        break;
      }
      default: {
        if (IsDigit(c0_)) {
          ScanNumber();
          type = Token::Type::NUMBER;
        } else if (IsIdentifierChar(c0_)) {
          type = ScanIdentifierOrKeyword();
        } else if (c0_ == kEndOfInput) {
          type = Token::Type::EOS;
        } else {
          SkipWhiteSpace();
          type = Token::Type::WHITESPACE;
        }
      }
    }
  } while (type == Token::Type::WHITESPACE);

  next_.type = type;
}

void Scanner::SkipWhiteSpace() {
  while (true) {
    switch (c0_) {
      case ' ':
      case '\r':
      case '\t': {
        Advance();
        break;
      }
      case '\n': {
        c0_pos_.line++;
        c0_pos_.column = 0;
        Advance();
        break;
      }
      default: { return; }
    }
  }
}

void Scanner::SkipLineComment() {
  while (c0_ != '\n' && c0_ != kEndOfInput) {
    Advance();
  }
}

void Scanner::SkipBlockComment() {
  // TODO(pmenon): Implement me
}

Token::Type Scanner::ScanIdentifierOrKeyword() {
  // First collect identifier
  int32_t identifier_char0 = c0_;
  while (IsIdentifierChar(c0_) && c0_ != kEndOfInput) {
    next_.literal += static_cast<char>(c0_);
    Advance();
  }

  if (identifier_char0 == '_' || IsInRange(identifier_char0, 'A', 'Z')) {
    // Definitely not keyword
    return Token::Type::IDENTIFIER;
  }

  const auto *identifier = next_.literal.data();
  auto identifier_len = static_cast<uint32_t>(next_.literal.length());

  return CheckIdentifierOrKeyword(identifier, identifier_len);
}

#define KEYWORDS()                          \
  GROUP_START('a')                          \
  GROUP_ELEM("and", Token::Type::AND)       \
  GROUP_START('e')                          \
  GROUP_ELEM("else", Token::Type::ELSE)     \
  GROUP_START('f')                          \
  GROUP_ELEM("false", Token::Type::FALSE)   \
  GROUP_ELEM("for", Token::Type::FOR)       \
  GROUP_ELEM("fun", Token::Type::FUN)       \
  GROUP_START('i')                          \
  GROUP_ELEM("if", Token::Type::IF)         \
  GROUP_START('n')                          \
  GROUP_ELEM("nil", Token::Type::NIL)       \
  GROUP_START('o')                          \
  GROUP_ELEM("or", Token::Type::OR)         \
  GROUP_START('r')                          \
  GROUP_ELEM("return", Token::Type::RETURN) \
  GROUP_START('s')                          \
  GROUP_ELEM("struct", Token::Type::STRUCT) \
  GROUP_START('t')                          \
  GROUP_ELEM("true", Token::Type::TRUE)     \
  GROUP_START('v')                          \
  GROUP_ELEM("var", Token::Type::VAR)       \
  GROUP_START('w')                          \
  GROUP_ELEM("while", Token::Type::WHILE)

Token::Type Scanner::CheckIdentifierOrKeyword(const char *input,
                                              uint32_t input_len) {
  static constexpr uint32_t kMinKeywordLen = 2;
  static constexpr uint32_t kMaxKeywordLen = 6;

  if (input_len < kMinKeywordLen || input_len > kMaxKeywordLen) {
    return Token::Type::IDENTIFIER;
  }

#define GROUP_START(c) \
  break;               \
  case c:

#define GROUP_ELEM(str, typ)                              \
  {                                                       \
    const uint64_t keyword_len = sizeof(str) - 1;         \
    if (keyword_len == input_len && str[1] == input[1] && \
        (keyword_len < 3 || str[2] == input[2]) &&        \
        (keyword_len < 4 || str[3] == input[3]) &&        \
        (keyword_len < 5 || str[4] == input[4]) &&        \
        (keyword_len < 6 || str[5] == input[5])) {        \
      return typ;                                         \
    }                                                     \
  }

  // The main switch statement that outlines all keywords
  switch (input[0]) {
    default:
      KEYWORDS()
  }

  // The input isn't a keyword, it must be an identifier
  return Token::Type::IDENTIFIER;
}

// hygiene
#undef GROUP_ELEM
#undef GROUP_START
#undef KEYWORDS

void Scanner::ScanNumber() {
  while (IsDigit(c0_)) {
    next_.literal += static_cast<char>(c0_);
    Advance();
  }

  if (c0_ == '.') {
    next_.literal.append(".");

    Advance();

    while (IsDigit(c0_)) {
      next_.literal += static_cast<char>(c0_);
      Advance();
    }
  }
}

Token::Type Scanner::ScanString() {
  // Single-line string. The lookahead character points to the start of the
  // string literal
  while (true) {
    if (c0_ == kEndOfInput) {
      next_.literal.clear();
      next_.literal = "Unterminated string";
      return Token::Type::ERROR;
    }

    // Is this character an escape?
    bool escape = (c0_ == '\\');

    // Add the character to the current string literal
    next_.literal += static_cast<char>(c0_);

    Advance();

    // If we see an enclosing quote and it hasn't been escaped, we're done
    if (c0_ == '"' && !escape) {
      Advance();
      return Token::Type::STRING;
    }
  }
}

}  // namespace tpl

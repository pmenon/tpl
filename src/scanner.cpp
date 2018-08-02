#include "scanner.h"

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

  // Advance character iterator to the first slot
  Advance();
  c0_pos_.line = 1;
  c0_pos_.column = 1;
}

const Scanner::TokenDesc &Scanner::Next() {
  Scan();
  return curr_;
}

void Scanner::Scan() {
  // Re-init the next token
  curr_.literal.clear();

  // The token
  Token::Type type;

  do {
    // Setup current token positions
    curr_.pos.line = c0_pos_.line;
    curr_.pos.column = c0_pos_.column;
    curr_.offset = offset_;

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
        ScanString();
        type = Token::Type::STRING;
        break;
      }
      default: {
        if (IsDigit(c0_)) {
          ScanNumber();
          type = Token::Type::NUMBER;
        } else if (IsIdentChar(c0_)) {
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

  curr_.type = type;
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
  while (IsIdentChar(c0_) && c0_ != kEndOfInput) {
    curr_.literal += static_cast<char>(c0_);
    Advance();
  }

  if (identifier_char0 == '_' || IsInRange(identifier_char0, 'A', 'Z')) {
    // Definitely not keyword
    return Token::Type::IDENTIFIER;
  }

  const auto *identifier = curr_.literal.data();
  auto identifier_len = static_cast<uint32_t>(curr_.literal.length());

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

    // hygiene
#undef GROUP_ELEM
#undef GROUP_START

  return Token::Type::IDENTIFIER;
}  // namespace tpl

#undef KEYWORDS

void Scanner::ScanNumber() {
  while (IsDigit(c0_)) {
    curr_.literal += static_cast<char>(c0_);
    Advance();
  }

  if (c0_ == '.') {
    curr_.literal.append(".");

    Advance();

    while (IsDigit(c0_)) {
      curr_.literal += static_cast<char>(c0_);
      Advance();
    }
  }
}

void Scanner::ScanString() {
  // Single-line string
  while (c0_ != kEndOfInput) {
    bool escape = (c0_ == '\\');

    curr_.literal += static_cast<char>(c0_);

    Advance();

    if (c0_ == '"' && !escape) {
      Advance();
      break;
    }
  }
}

}  // namespace tpl

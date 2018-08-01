#include "scanner.h"

#include <scanner.h>
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
        } else if (IsAlpha(c0_)) {
          ScanIdentifier();
          type = Token::Type::IDENTIFIER;
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

void Scanner::ScanIdentifier() {}

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

#include "scanner.h"

#include <scanner.h>
#include <cassert>
#include <stdexcept>

namespace tpl {

Scanner::Scanner() : source_(nullptr), len_(0), pos_(0), line_(1) {}

void Scanner::Initialize(const char *source, uint64_t len) {
  source_ = source;
  len_ = len;
  pos_ = 0;
  line_ = 1;

  // Setup current token information
  curr_.type = Token::Type::UNINIITIALIZED;
  curr_.loc.begin = 0;
  curr_.loc.end = 0;
  curr_.loc.line_ = 1;

  // Setup the next token information
  next_.type = Token::Type::UNINIITIALIZED;
  next_.loc.begin = 0;
  next_.loc.end = 0;
  next_.loc.line_ = 1;

  // Advance character iterator to the first slot
  Advance();

  // Find first token
  Scan();
}

Token::Type Scanner::Next() {
  curr_ = next_;
  Scan();
  return curr_.type;
}

void Scanner::Scan() {
  // Track begin position
  uint64_t begin;

  // The token
  Token::Type type;

  do {
    begin = source_position();

    switch (c_) {
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
        type = Token::Type::EQUAL;
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
        if (c_ == '/') {
          SkipWhiteSpace();
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
      default: {
        if (c_ == kEndOfInput) {
          type = Token::Type::EOS;
        } else {
          SkipWhiteSpace();
          type = Token::Type::WHITESPACE;
        }
      }
    }
  } while (type == Token::Type::WHITESPACE);

  // Track end position
  uint64_t end = source_position();

  next_.type = type;
  next_.loc.begin = begin;
  next_.loc.end = end;
  next_.loc.line_ = line();
}

void Scanner::SkipWhiteSpace() {
  while (true) {
    switch (c_) {
      case ' ':
      case '\r':
      case '\t': {
        Advance();
        break;
      }
      case '/': {
        // Single-line comment
        while (c_ != '\n' && c_ != kEndOfInput) {
          Advance();
        }
        return;
      }
      case '\n': {
        line_++;
        Advance();
        break;
      }
      default: { return; }
    }
  }
}

}  // namespace tpl

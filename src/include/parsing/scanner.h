#pragma once

#include <cstdint>
#include <string>

#include "parsing/token.h"
#include "util/macros.h"

namespace tpl::parsing {

class Scanner {
  static constexpr int32_t kEndOfInput = -1;

 public:
  /**
   * Describes the position in the original source in terms of lines and columns
   */
  struct SourcePosition {
    uint64_t line;
    uint64_t column;
  };

  /**
   * Describes information about a token including its type, its position in
   * the origin source, and a literal if it has one
   */
  struct TokenDesc {
    Token::Type type;
    SourcePosition pos;
    uint64_t offset;
    std::string literal;
  };

  Scanner(const char *source, uint64_t source_len);

  DISALLOW_COPY_AND_MOVE(Scanner);

  /**
   * Read the next token in the source input stream
   * @return The next token
   */
  Token::Type Next();

  /**
   * Peek/look at the next token in the stream without consuming it
   * @return The next token
   */
  Token::Type peek() { return next_.type; }

  const std::string &current_literal() const { return curr_.literal; }
  uint64_t current_raw_pos() const { return curr_.offset; }

 private:
  // Scan the next token
  void Scan();

  // Advance a single character into the source input stream
  void Advance() {
    bool at_end = (offset_ == source_len_);
    if (TPL_UNLIKELY(at_end)) {
      c0_ = kEndOfInput;
      return;
    }

    // Not at end, bump
    c0_ = source_[offset_++];
    c0_pos_.column++;
  }

  // Does the current character match the expected? If so, advance the scanner
  bool Matches(int32_t expected) {
    if (c0_ != expected) {
      return false;
    }

    Advance();

    return true;
  }

  // Skip whitespace from the current token to the next valid token
  void SkipWhiteSpace();

  // Skip a line comment
  void SkipLineComment();

  // Skip a block comment
  void SkipBlockComment();

  // Scan an identifier token
  Token::Type ScanIdentifierOrKeyword();

  // Check if the given input is a keyword or an identifier
  Token::Type CheckIdentifierOrKeyword(const char *input, uint32_t input_len);

  // Scan a number literal
  void ScanNumber();

  // Scan a string literal
  Token::Type ScanString();

 private:
  // Is the current character a character?
  static bool IsInRange(int32_t c, int32_t lower, int32_t upper) {
    return (c >= lower && c <= upper);
  }

  // Is the provided character an alphabetic character
  static bool IsAlpha(int32_t c) {
    return IsInRange(c, 'a', 'z') || IsInRange(c, 'A', 'Z');
  }

  // Is the current character a digit?
  static bool IsDigit(int32_t c) { return IsInRange(c, '0', '9'); }

  // Is this character allowed in an identifier?
  static bool IsIdentifierChar(int32_t c) {
    return IsAlpha(c) || IsDigit(c) || c == '_';
  }

 private:
  // The source input and its length
  const char *source_;
  uint64_t source_len_;

  // The current position in the source
  uint64_t offset_;

  // The lookahead character
  int32_t c0_;
  SourcePosition c0_pos_;

  // Information about the current token
  TokenDesc curr_;
  TokenDesc next_;
};

}  // namespace tpl::parsing
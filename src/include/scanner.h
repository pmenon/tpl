#pragma once

#include <cstdint>
#include <string>

#include "macros.h"
#include "token.h"

namespace tpl {

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
   * Describes information about a token including it's type, its position in
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

  // Read the next token in the source input stream
  const TokenDesc &Next();

  const TokenDesc &current_token() const { return curr_; }
  const SourcePosition &source_position() { return c0_pos_; }
  uint64_t current_offset() const { return offset_; }

 private:
  // Scan the next token
  void Scan();

  // Advance a single character into the source input stream
  void Advance();

  // Does the current character match the expected? If so, advance the scanner
  bool Matches(int32_t expected);

  // Skip whitespace from the current token to the next valid token
  void SkipWhiteSpace();

  // Skip a line comment
  void SkipLineComment();

  // Skip a block comment
  void SkipBlockComment();

  // Scan an identifier token
  void ScanIdentifier();

  // Scan a number literal
  void ScanNumber();

  // Scan a string literal
  void ScanString();

 private:
  // Is the current character a character?
  static bool IsAlpha(int32_t c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c == '_');
  }

  // Is the current character a digit?
  static bool IsDigit(int32_t c) { return c >= '0' && c <= '9'; }

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
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline void Scanner::Advance() {
  bool at_end = (offset_ == source_len_);
  if (TPL_UNLIKELY(at_end)) {
    c0_ = kEndOfInput;
    return;
  }

  c0_ = source_[offset_++];
  c0_pos_.column++;
}

inline bool Scanner::Matches(int32_t expected) {
  if (c0_ != expected) {
    return false;
  }

  Advance();

  return true;
}

}  // namespace tpl
#pragma once

#include <cstdint>

#include "macros.h"
#include "token.h"

namespace tpl {

class Scanner {
  static constexpr int32_t kEndOfInput = -1;

  using Char = int32_t;

 public:
  Scanner();

  DISALLOW_COPY_AND_MOVE(Scanner);

  // Perform common initialization logic for this scanner
  void Initialize(const char *source, uint64_t len);

  // Read the next token in the source input stream
  Token::Type Next();

  /**
   * Handle for a location in the source
   */
  struct Location {
    uint64_t line_;
    uint64_t begin;
    uint64_t end;
  };

  const Location &current_location() const { return curr_.loc; }
  uint64_t source_position() const { return pos_; }
  uint64_t line() const { return line_; }

 private:
  struct TokenInfo {
    Token::Type type;
    Location loc;
  };

  // Scan the next TPL token
  void Scan();

  // Advance a single character into the source input stream
  void Advance() {
    bool at_end = (pos_ == len_);
    if (TPL_UNLIKELY(at_end)) {
      c_ = kEndOfInput;
      return;
    }

    pos_++;
    c_ = source_[pos_ - 1];
  }

  // Skip whitespace from the current token to the next valid token
  void SkipWhiteSpace();

 private:
  // The source input and its length
  const char *source_;
  uint64_t len_;

  // The current position in the source
  uint64_t pos_;

  // The current character
  Char c_;

  // The current line (in the source) we're on
  uint64_t line_;

  // Information about the current token
  TokenInfo curr_;
  TokenInfo next_;
};

}  // namespace tpl
#pragma once

#include <vector>

#include "ast/identifier.h"
#include "common.h"
#include "parsing/token.h"
#include "sema/error_message.h"

namespace tpl::sema {

/**
 * TODO(pmenon): These don't use region vectors ...
 */
class ErrorReporter {
 public:
  // Record an error
  template <typename... ArgTypes>
  void Report(const SourcePosition &pos,
              const ErrorMessage<ArgTypes...> &message, ArgTypes... args) {
    std::vector<MessageArgument> typed_args = {
        MessageArgument(std::move(args))...};
    errors_.emplace_back(pos, message.id, std::move(typed_args));
  }

  // Have any errors been reported?
  bool has_errors() const { return !errors_.empty(); }

  void PrintErrors();

 private:
  /*
   * A single argument in the error message
   */
  class MessageArgument {
   public:
    enum Kind { CString, Int, Token, Position };

    explicit MessageArgument(const char *str)
        : kind_(Kind::CString), raw_str_(str) {}

    explicit MessageArgument(int32_t integer)
        : kind_(Kind::Int), integer_(integer) {}

    explicit MessageArgument(ast::Identifier str)
        : MessageArgument(str.data()) {}

    explicit MessageArgument(parsing::Token::Type type)
        : MessageArgument(
              static_cast<std::underlying_type_t<parsing::Token::Type>>(type)) {
      kind_ = Kind::Token;
    }

    explicit MessageArgument(const SourcePosition &pos)
        : kind_(Kind::Position), pos_(pos) {}

    Kind kind() const { return kind_; }

   private:
    friend class ErrorReporter;
    void FormatMessageArgument(std::string &str) const;

   private:
    Kind kind_;
    union {
      const char *raw_str_;
      int32_t integer_;
      SourcePosition pos_;
    };
  };

  /*
   * An encapsulated error message with proper argument types that can be
   * formatted and printed.
   */
  class MessageWithArgs {
   public:
    MessageWithArgs(const SourcePosition &pos, ErrorMessageId id,
                    std::vector<MessageArgument> &&args)
        : pos_(pos), id_(id), args_(std::move(args)) {}

    const SourcePosition &position() const { return pos_; }

    ErrorMessageId error_message_id() const { return id_; }

   private:
    friend class ErrorReporter;
    std::string FormatMessage() const;

   private:
    const SourcePosition pos_;
    ErrorMessageId id_;
    std::vector<MessageArgument> args_;
  };

 private:
  std::vector<MessageWithArgs> errors_;
};

}  // namespace tpl::sema
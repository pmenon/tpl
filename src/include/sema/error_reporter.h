#pragma once

#include <vector>

#include "ast/identifier.h"
#include "common.h"
#include "parsing/token.h"
#include "sema/error_message.h"

namespace tpl {

namespace ast {
class Type;
}  // namespace ast

namespace sema {

namespace detail {
template <typename T>
struct PassArgument {
  typedef T type;
};

}  // namespace detail

/**
 * TODO(pmenon): These don't use region vectors ...
 */
class ErrorReporter {
 public:
  // Record an error
  template <typename... ArgTypes>
  void Report(const SourcePosition &pos,
              const ErrorMessage<ArgTypes...> &message,
              typename detail::PassArgument<ArgTypes>::type... args) {
    errors_.emplace_back(pos, message, std::forward<ArgTypes>(args)...);
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
    enum Kind { CString, Int, Position, Token, Type };

    explicit MessageArgument(const char *str)
        : kind_(Kind::CString), raw_str_(str) {}

    explicit MessageArgument(int32_t integer)
        : kind_(Kind::Int), integer_(integer) {}

    explicit MessageArgument(ast::Identifier str)
        : MessageArgument(str.data()) {}

    explicit MessageArgument(ast::Type *type)
        : kind_(Kind::Type), type_(type) {}

    explicit MessageArgument(const parsing::Token::Type type)
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
      ast::Type *type_;
    };
  };

  /*
   * An encapsulated error message with proper argument types that can be
   * formatted and printed.
   */
  class MessageWithArgs {
   public:
    template <typename... ArgTypes>
    MessageWithArgs(const SourcePosition &pos,
                    const ErrorMessage<ArgTypes...> &message,
                    typename detail::PassArgument<ArgTypes>::type... args)
        : pos_(pos), id_(message.id) {
      std::vector<MessageArgument> full_args = {
          MessageArgument(std::move(args))...};
      args_ = std::move(full_args);
    }

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

}  // namespace sema
}  // namespace tpl
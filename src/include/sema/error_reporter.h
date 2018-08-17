#pragma once

#include <vector>

#include "common.h"
#include "sema/error_message.h"

namespace tpl::sema {

/**
 * TODO(pmenon): These don't use region vectors ...
 */
class ErrorReporter {
 public:
  // Record an error
  template <typename... ArgTypes>
  void Report(ErrorMessage<ArgTypes...> message, ArgTypes... args) {
    std::vector<SingleArg> typed_args = {std::move(args)...};
    errors_.emplace_back(message.id, std::move(typed_args));
  }

  // Have any errors been reported?
  bool has_errors() const { return !errors_.empty(); }

 private:
  /*
   * A single argument in the error message
   */
  class SingleArg {
   public:
    enum Kind {
      CString,
      Int,
    };

    explicit SingleArg(const char *str) : kind_(Kind::CString), c_str_(str) {}

    explicit SingleArg(uint32_t integer)
        : kind_(Kind::Int), integer_(integer) {}

    Kind kind() const { return kind_; }

   private:
    Kind kind_;
    union {
      const char *c_str_;
      uint32_t integer_;
    };
  };

  /*
   * An encapsulated error message with proper argument types that can be
   * formatted and printed.
   */
  class MessageWithArgs {
   public:
    MessageWithArgs(ErrorMessageId id, std::vector<SingleArg> &&args)
        : id_(id), args_(std::move(args)) {}

    ErrorMessageId error_message_id() const { return id_; }

   private:
    ErrorMessageId id_;
    std::vector<SingleArg> args_;
  };

 private:
  std::vector<MessageWithArgs> errors_;
};

}  // namespace tpl::sema
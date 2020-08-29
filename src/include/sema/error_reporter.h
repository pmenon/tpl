#pragma once

#include <iosfwd>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "ast/identifier.h"
#include "common/common.h"
#include "parsing/token.h"
#include "sema/error_message.h"
#include "util/region_containers.h"

namespace tpl {

namespace ast {
class Type;
}  // namespace ast

namespace sema {

namespace detail {

template <typename T>
struct PassArgument {
  using type = T;
};

}  // namespace detail

/**
 * Utility class to register and store diagnostic error messages generated during parsing and
 * semantic analysis.
 */
class ErrorReporter {
 public:
  /**
   * Create a new error reporter.
   */
  ErrorReporter() : region_("error-strings"), errors_(&region_) {}

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(ErrorReporter);

  /**
   * Report an error.
   * @tparam ArgTypes Types of all arguments.
   * @param pos The position in the source file the error occurs.
   * @param message The error message.
   * @param args The arguments.
   */
  template <typename... ArgTypes>
  void Report(const SourcePosition &pos, const ErrorMessage<ArgTypes...> &message,
              typename detail::PassArgument<ArgTypes>::type... args) {
    errors_.emplace_back(&region_, pos, message, std::forward<ArgTypes>(args)...);
  }

  /**
   * @return True if any errors have been reported; false otherwise.
   */
  bool HasErrors() const { return !errors_.empty(); }

  /**
   * Reset this error reporter to as if just after construction. This clears all pending error
   * messages.
   */
  void Reset() { errors_.clear(); }

  /**
   * Dump all error messages to the given output stream.
   * @param os The stream to write errors into.
   */
  void PrintErrors(std::ostream &os);

 private:
  // A single argument in the error message.
  class MessageArgument {
   public:
    explicit MessageArgument(const char *str) : arg_(str) {}

    explicit MessageArgument(ast::Identifier str) : MessageArgument(str.GetData()) {}

    explicit MessageArgument(SourcePosition pos) : arg_(pos) {}

    explicit MessageArgument(int32_t integer) : arg_(integer) {}

    explicit MessageArgument(ast::Type *type) : arg_(type) {}

    explicit MessageArgument(parsing::Token::Type type) : arg_(type) {}

   private:
    friend class ErrorReporter;
    void FormatMessageArgument(std::string &str) const;

   private:
    std::variant<const char *, int32_t, SourcePosition, parsing::Token::Type, ast::Type *> arg_;
  };

  // An encapsulated error message with proper argument types that can be
  // formatted and printed.
  class MessageWithArgs {
   public:
    template <typename... ArgTypes>
    MessageWithArgs(util::Region *region, const SourcePosition &pos,
                    const ErrorMessage<ArgTypes...> &message,
                    typename detail::PassArgument<ArgTypes>::type... args)
        : pos_(pos), id_(message.id), args_(region) {
      args_.insert(args_.end(), {MessageArgument(std::move(args))...});
    }

    const SourcePosition &position() const { return pos_; }

    ErrorMessageId error_message_id() const { return id_; }

   private:
    friend class ErrorReporter;
    std::string FormatMessage() const;

   private:
    const SourcePosition pos_;
    ErrorMessageId id_;
    util::RegionVector<MessageArgument> args_;
  };

 private:
  // Memory region
  util::Region region_;

  // List of all errors
  util::RegionVector<MessageWithArgs> errors_;
};

}  // namespace sema
}  // namespace tpl

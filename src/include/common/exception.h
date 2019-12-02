#pragma once

#include <cstdarg>
#include <stdexcept>
#include <string>

#include "spdlog/fmt/fmt.h"

#include "common/macros.h"
#include "sql/sql.h"

namespace tpl {

/**
 * All exception types.
 *
 * NOTE: Please keep these sorted.
 */
enum class ExceptionType {
  Cardinality,     // vectors have different cardinalities
  Conversion,      // conversion/casting error
  Decimal,         // decimal related
  DivideByZero,    // divide by 0
  Execution,       // executor related
  File,            // file related
  Index,           // index related
  InvalidType,     // incompatible for operation
  NotImplemented,  // method not implemented
  OutOfRange,      // value out of range error
  TypeMismatch,    // mismatched types
  UnknownType,     // unknown type
};

/**
 * Base exception class.
 */
class Exception : public std::exception {
 public:
  /**
   * Construct an exception with the given type and message.
   * @param exception_type The type of the exception.
   * @param message The message to print.
   */
  Exception(ExceptionType exception_type, const std::string &message);

  /**
   * Return the error message.
   * @return
   */
  const char *what() const noexcept override;

  /**
   * Convert an exception type into its string representation.
   * @param type The type of exception.
   * @return The string name of the input exception type.
   */
  static std::string ExceptionTypeToString(ExceptionType type);

 protected:
  template <typename... Args>
  void Format(const Args &... args) {
    exception_message_ = fmt::format(exception_message_, args...);
  }

 private:
  // The type of the exception
  ExceptionType type_;

  // The message
  std::string exception_message_;
};

std::ostream &operator<<(std::ostream &os, const Exception &e);

// ---------------------------------------------------------
// Concrete exceptions
// ---------------------------------------------------------

/**
 * An exception thrown due to an invalid cast.
 */
class CastException : public Exception {
 public:
  CastException(sql::TypeId src_type, sql::TypeId dest_type)
      : Exception(ExceptionType::Conversion, "Type {} cannot be cast as {}") {
    Format(TypeIdToString(src_type), TypeIdToString(dest_type));
  }
};

/**
 * An exception thrown when a given type cannot be converted into another type.
 */
class ConversionException : public Exception {
 public:
  template <typename... Args>
  explicit ConversionException(const std::string &msg, const Args &... args)
      : Exception(ExceptionType::Conversion, msg) {
    Format(args...);
  }
};

/**
 * The given type is invalid in its context of use.
 */
class InvalidTypeException : public Exception {
 public:
  InvalidTypeException(sql::TypeId type, const std::string &msg)
      : Exception(ExceptionType::InvalidType, "Invalid type ['{}']: " + msg) {
    Format(TypeIdToString(type));
  }
};

/**
 * An exception thrown to indicate some functionality isn't implemented.
 */
class NotImplementedException : public Exception {
 public:
  template <typename... Args>
  explicit NotImplementedException(const std::string &msg, const Args &... args)
      : Exception(ExceptionType::NotImplemented, msg) {
    Format(args...);
  }
};

/**
 * An unexpected type enters.
 */
class TypeMismatchException : public Exception {
 public:
  TypeMismatchException(sql::TypeId src_type, sql::TypeId dest_type, const std::string &msg)
      : Exception(ExceptionType::TypeMismatch, "Type '{}' does not match type '{}'. " + msg) {
    Format(TypeIdToString(src_type), TypeIdToString(dest_type));
  }
};

/**
 * An exception thrown when a value falls outside a given type's valid value range.
 */
class ValueOutOfRangeException : public Exception {
 public:
  template <typename T, typename = std::enable_if_t<std::is_arithmetic_v<T>, uint32_t>>
  ValueOutOfRangeException(T value, sql::TypeId src_type, sql::TypeId dest_type)
      : ValueOutOfRangeException() {
    Format(TypeIdToString(src_type), TypeIdToString(dest_type));
  }

 private:
  ValueOutOfRangeException()
      : Exception(ExceptionType::OutOfRange,
                  "Type {} with value {} cannot be cast because the value is out of range for the "
                  "target type {}") {}
};

}  // namespace tpl

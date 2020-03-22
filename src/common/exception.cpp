#include "common/exception.h"

#include <iostream>
#include <string>

namespace tpl {

Exception::Exception(ExceptionType exception_type, const std::string &message)
    : type_(exception_type) {
  exception_message_ = ExceptionTypeToString(type_) + ": " + message;
}

const char *Exception::what() const noexcept { return exception_message_.c_str(); }

// static
std::string Exception::ExceptionTypeToString(ExceptionType type) {
  switch (type) {
    case ExceptionType::OutOfRange:
      return "Out of Range";
    case ExceptionType::Conversion:
      return "Conversion";
    case ExceptionType::UnknownType:
      return "Unknown Type";
    case ExceptionType::Decimal:
      return "Decimal";
    case ExceptionType::DivideByZero:
      return "Divide by Zero";
    case ExceptionType::InvalidType:
      return "Invalid type";
    case ExceptionType::NotImplemented:
      return "Not implemented";
    case ExceptionType::Execution:
      return "Executor";
    case ExceptionType::Index:
      return "Index";
    default:
      return "Unknown";
  }
}
std::ostream &operator<<(std::ostream &os, const Exception &e) { return os << e.what(); }

}  // namespace tpl

#include "sema/error_reporter.h"

#include <cstring>

#include "ast/type.h"
#include "ast/type_visitor.h"

namespace tpl::sema {

namespace {
#define F(id, str, arg_types) str,
constexpr const char *error_strings[] = {MESSAGE_LIST(F)};
#undef F

class TypePrinter : public ast::TypeVisitor<TypePrinter> {
 public:
#define DECLARE_VISIT_TYPE(Type) void Visit##Type(ast::Type *type);
  TYPE_LIST(DECLARE_VISIT_TYPE)
#undef DECLARE_VISIT_TYPE

  DEFINE_AST_VISITOR_METHOD()

  const std::string &Print(ast::Type *type) {
    Visit(type);
    return result();
  }

  const std::string &result() const { return res_; }

 private:
  std::string res_;
};

void tpl::sema::TypePrinter::VisitIntegerType(ast::IntegerType *type) {
  switch (type->int_kind()) {
    case ast::IntegerType::IntKind::Int8: {
      res_.append("int8");
      break;
    }
    case ast::IntegerType::IntKind::Int16: {
      res_.append("int16");
      break;
    }
    case ast::IntegerType::IntKind::Int32: {
      res_.append("int32");
      break;
    }
    case ast::IntegerType::IntKind::Int64: {
      res_.append("int64");
      break;
    }
    case ast::IntegerType::IntKind::UInt8: {
      res_.append("uint8");
      break;
    }
    case ast::IntegerType::IntKind::UInt16: {
      res_.append("uint16");
      break;
    }
    case ast::IntegerType::IntKind::UInt32: {
      res_.append("uint32");
      break;
    }
    case ast::IntegerType::IntKind::UInt64: {
      res_.append("uint64");
      break;
    }
  }
}

void tpl::sema::TypePrinter::VisitFunctionType(ast::FunctionType *type) {
  res_.append("(");
  bool first = true;
  for (auto *param : type->params()) {
    if (!first) res_.append(",");
    first = false;
    Visit(param);
  }
  res_.append(")->");
  Visit(type->return_type());
}

void tpl::sema::TypePrinter::VisitBoolType(ast::BoolType *type) {
  res_.append("bool");
}

void tpl::sema::TypePrinter::VisitPointerType(ast::PointerType *type) {
  res_.append("*");
  Visit(type->base());
}

void tpl::sema::TypePrinter::VisitFloatType(ast::FloatType *type) {
  switch (type->float_kind()) {
    case ast::FloatType::FloatKind::Float32: {
      res_.append("float32");
      break;
    }
    case ast::FloatType::FloatKind::Float64: {
      res_.append("fload64");
      break;
    }
  }
}

void tpl::sema::TypePrinter::VisitStructType(ast::StructType *type) {
  res_.append("struct{");
  bool first = true;
  for (auto *field : type->fields()) {
    if (!first) res_.append(",");
    first = false;
    Visit(field);
  }
  res_.append("}");
}

void tpl::sema::TypePrinter::VisitNilType(ast::NilType *type) {
  res_.append("nil");
}

void tpl::sema::TypePrinter::VisitArrayType(ast::ArrayType *type) {
  res_.append("[");
  if (type->length() != 0) {
    res_.append(std::to_string(type->length()));
  }
  res_.append("]");
  Visit(type->element_type());
}

}  // namespace

void ErrorReporter::MessageArgument::FormatMessageArgument(
    std::string &str) const {
  switch (kind()) {
    case Kind::CString: {
      str.append(raw_str_);
      break;
    }
    case MessageArgument::Kind::Int: {
      str.append(std::to_string(integer_));
      break;
    }
    case Kind::Position: {
      str.append("[line/col: ")
          .append(std::to_string(pos_.line))
          .append("/")
          .append(std::to_string(pos_.column))
          .append("]");
      break;
    }
    case Kind::Token: {
      str.append(
          parsing::Token::String(static_cast<parsing::Token::Type>(integer_)));
      break;
    }
    case Kind::Type: {
      TypePrinter type_printer;
      str.append(type_printer.Print(type_));
      break;
    }
  }
}

std::string ErrorReporter::MessageWithArgs::FormatMessage() const {
  std::string msg;

  auto msg_idx =
      static_cast<std::underlying_type_t<ErrorMessageId>>(error_message_id());

  msg.append("Line: ").append(std::to_string(position().line)).append(", ");
  msg.append("Col: ").append(std::to_string(position().column)).append(" => ");

  const char *fmt = error_strings[msg_idx];
  if (args_.empty()) {
    msg.append(fmt);
    return msg;
  }

  // Need to format
  uint32_t arg_idx = 0;
  while (fmt != nullptr) {
    const char *pos = strchr(fmt, '%');
    if (pos == nullptr) {
      msg.append(fmt);
      break;
    }

    msg.append(fmt, pos - fmt);

    args_[arg_idx++].FormatMessageArgument(msg);

    fmt = pos + 1;
    while (std::isalnum(*fmt)) {
      fmt++;
    }
  }

  return msg;
}

void ErrorReporter::PrintErrors() {
  std::string error_str;

  for (const auto &error : errors_) {
    error_str.append(error.FormatMessage()).append("\n");
  }

  fprintf(stderr, "%s", error_str.c_str());
}

}  // namespace tpl::sema
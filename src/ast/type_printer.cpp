#include "ast/type.h"

#include <string>

#include "llvm/ADT/SmallString.h"
#include "llvm/Support/raw_ostream.h"

#include "ast/type_visitor.h"

namespace tpl::ast {

namespace {

/**
 * Visitor class that walks a type hierarchy tree with the purpose of
 * pretty-printing to an injected output stream.
 */
class TypePrinter : public TypeVisitor<TypePrinter> {
 public:
  explicit TypePrinter(llvm::raw_ostream &out) : out_(out) {}

#define DECLARE_VISIT_TYPE(Type) void Visit##Type(const Type *type);
  TYPE_LIST(DECLARE_VISIT_TYPE)
#undef DECLARE_VISIT_TYPE

  void Print(const Type *type) { Visit(type); }

 private:
  llvm::raw_ostream &os() { return out_; }

 private:
  llvm::raw_ostream &out_;
};

void TypePrinter::VisitIntegerType(const IntegerType *type) {
  switch (type->int_kind()) {
    case IntegerType::IntKind::Int8: {
      os() << "int8";
      break;
    }
    case IntegerType::IntKind::Int16: {
      os() << "int16";
      break;
    }
    case IntegerType::IntKind::Int32: {
      os() << "int32";
      break;
    }
    case IntegerType::IntKind::Int64: {
      os() << "int64";
      break;
    }
    case IntegerType::IntKind::UInt8: {
      os() << "uint8";
      break;
    }
    case IntegerType::IntKind::UInt16: {
      os() << "uint16";
      break;
    }
    case IntegerType::IntKind::UInt32: {
      os() << "uint32";
      break;
    }
    case IntegerType::IntKind::UInt64: {
      os() << "uint64";
      break;
    }
  }
}

void TypePrinter::VisitFunctionType(const FunctionType *type) {
  os() << "(";
  bool first = true;
  for (const auto &param : type->params()) {
    if (!first) os() << ",";
    first = false;
    Visit(param.type);
  }
  os() << ")->";
  Visit(type->return_type());
}

void TypePrinter::VisitBoolType(const BoolType *type) { os() << "bool"; }

void TypePrinter::VisitStringType(const StringType *type) { os() << "string"; }

void TypePrinter::VisitPointerType(const PointerType *type) {
  os() << "*";
  Visit(type->base());
}

void TypePrinter::VisitFloatType(const FloatType *type) {
  switch (type->float_kind()) {
    case FloatType::FloatKind::Float32: {
      os() << "f32";
      break;
    }
    case FloatType::FloatKind::Float64: {
      os() << "f64";
      break;
    }
  }
}

void TypePrinter::VisitStructType(const StructType *type) {
  os() << "struct{";
  bool first = true;
  for (const auto &field : type->fields()) {
    if (!first) os() << ",";
    first = false;
    Visit(field.type);
  }
  os() << "}";
}

void TypePrinter::VisitNilType(const NilType *type) { os() << "nil"; }

void TypePrinter::VisitArrayType(const ArrayType *type) {
  os() << "[";
  if (type->length() != 0) {
    os() << type->length();
  }
  os() << "]";
  Visit(type->element_type());
}

void TypePrinter::VisitInternalType(const InternalType *type) {
  os() << llvm::StringRef(type->name().data());
}

void tpl::ast::TypePrinter::VisitSqlType(const SqlType *type) {
  os() << type->sql_type().GetName();
}

void tpl::ast::TypePrinter::VisitMapType(const MapType *type) {
  os() << "map[";
  Visit(type->key_type());
  os() << "]";
  Visit(type->value_type());
}

}  // namespace

// static
std::string Type::ToString(const Type *type) {
  llvm::SmallString<256> buffer;
  llvm::raw_svector_ostream stream(buffer);

  TypePrinter printer(stream);
  printer.Print(type);

  return buffer.str();
}

}  // namespace tpl::ast

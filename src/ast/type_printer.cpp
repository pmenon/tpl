#include "ast/type.h"

#include "llvm/ADT/SmallString.h"
#include "llvm/Support/raw_ostream.h"

#include "ast/type_visitor.h"

namespace tpl::ast {

class TypePrinter : public TypeVisitor<TypePrinter> {
 public:
  explicit TypePrinter(llvm::raw_ostream &out) : out_(out) {}

#define DECLARE_VISIT_TYPE(Type) void Visit##Type(const Type *type);
  TYPE_LIST(DECLARE_VISIT_TYPE)
#undef DECLARE_VISIT_TYPE

  void Print(const Type *type) { Visit(type); }

 private:
  llvm::raw_ostream &out_;
};

void TypePrinter::VisitIntegerType(const IntegerType *type) {
  switch (type->int_kind()) {
    case IntegerType::IntKind::Int8: {
      out_ << "i8";
      break;
    }
    case IntegerType::IntKind::Int16: {
      out_ << "i16";
      break;
    }
    case IntegerType::IntKind::Int32: {
      out_ << "i32";
      break;
    }
    case IntegerType::IntKind::Int64: {
      out_ << "i64";
      break;
    }
    case IntegerType::IntKind::UInt8: {
      out_ << "u8";
      break;
    }
    case IntegerType::IntKind::UInt16: {
      out_ << "u16";
      break;
    }
    case IntegerType::IntKind::UInt32: {
      out_ << "u32";
      break;
    }
    case IntegerType::IntKind::UInt64: {
      out_ << "u64";
      break;
    }
  }
}

void TypePrinter::VisitFunctionType(const FunctionType *type) {
  out_ << "(";
  bool first = true;
  for (auto *param : type->params()) {
    if (!first) out_ << ",";
    first = false;
    Visit(param);
  }
  out_ << ")->";
  Visit(type->return_type());
}

void TypePrinter::VisitBoolType(const BoolType *type) { out_ << "bool"; }

void TypePrinter::VisitPointerType(const PointerType *type) {
  out_ << "*";
  Visit(type->base());
}

void TypePrinter::VisitFloatType(const FloatType *type) {
  switch (type->float_kind()) {
    case FloatType::FloatKind::Float32: {
      out_ << "f32";
      break;
    }
    case FloatType::FloatKind::Float64: {
      out_ << "f64";
      break;
    }
  }
}

void TypePrinter::VisitStructType(const StructType *type) {
  out_ << "struct{";
  bool first = true;
  for (auto *field : type->fields()) {
    if (!first) out_ << ",";
    first = false;
    Visit(field);
  }
  out_ << "}";
}

void TypePrinter::VisitNilType(const NilType *type) { out_ << "nil"; }

void TypePrinter::VisitArrayType(const ArrayType *type) {
  out_ << "[";
  if (type->length() != 0) {
    out_ << type->length();
  }
  out_ << "]";
  Visit(type->element_type());
}

std::string Type::ToString(const Type *type) {
  llvm::SmallString<256> buffer;
  llvm::raw_svector_ostream stream(buffer);

  TypePrinter printer(stream);
  printer.Print(type);

  return buffer.str();
}

}  // namespace tpl::ast
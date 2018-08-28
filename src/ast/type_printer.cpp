#include "ast/type.h"

#include "llvm/ADT/SmallString.h"
#include "llvm/Support/raw_ostream.h"

#include "ast/type_visitor.h"

namespace tpl::ast {

class TypePrinter : public TypeVisitor<TypePrinter> {
 public:
  explicit TypePrinter(llvm::raw_ostream &out) : out_(out) {}

#define DECLARE_VISIT_TYPE(Type) void Visit##Type(Type *type);
  TYPE_LIST(DECLARE_VISIT_TYPE)
#undef DECLARE_VISIT_TYPE

  void Print(Type *type) {
    Visit(type);
  }

 private:
  llvm::raw_ostream &out_;
};

void TypePrinter::VisitIntegerType(IntegerType *type) {
  switch (type->int_kind()) {
    case IntegerType::IntKind::Int8: {
      out_ << "int8";
      break;
    }
    case IntegerType::IntKind::Int16: {
      out_ << "int16";
      break;
    }
    case IntegerType::IntKind::Int32: {
      out_ << "int32";
      break;
    }
    case IntegerType::IntKind::Int64: {
      out_ << "int64";
      break;
    }
    case IntegerType::IntKind::UInt8: {
      out_ << "uint8";
      break;
    }
    case IntegerType::IntKind::UInt16: {
      out_ << "uint16";
      break;
    }
    case IntegerType::IntKind::UInt32: {
      out_ << "uint32";
      break;
    }
    case IntegerType::IntKind::UInt64: {
      out_ << "uint64";
      break;
    }
  }
}

void TypePrinter::VisitFunctionType(FunctionType *type) {
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

void TypePrinter::VisitBoolType(BoolType *type) { out_ << "bool"; }

void TypePrinter::VisitPointerType(PointerType *type) {
  out_ << "*";
  Visit(type->base());
}

void TypePrinter::VisitFloatType(FloatType *type) {
  switch (type->float_kind()) {
    case FloatType::FloatKind::Float32: {
      out_ << "float32";
      break;
    }
    case FloatType::FloatKind::Float64: {
      out_ << "float64";
      break;
    }
  }
}

void TypePrinter::VisitStructType(StructType *type) {
  out_ << "struct{";
  bool first = true;
  for (auto *field : type->fields()) {
    if (!first) out_ << ",";
    first = false;
    Visit(field);
  }
  out_ << "}";
}

void TypePrinter::VisitNilType(NilType *type) { out_ << "nil"; }

void TypePrinter::VisitArrayType(ArrayType *type) {
  out_ << "[";
  if (type->length() != 0) {
    out_ << type->length();
  }
  out_ << "]";
  Visit(type->element_type());
}

std::string Type::GetAsString(Type *type) {
  llvm::SmallString<256> buffer;
  llvm::raw_svector_ostream stream(buffer);

  TypePrinter printer(stream);
  printer.Print(type);

  return buffer.str();
}

}  // namespace tpl::ast
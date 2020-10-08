#include <string>

#include "ast/type.h"
#include "ast/type_visitor.h"
#include "llvm/ADT/SmallString.h"
#include "llvm/Support/raw_ostream.h"

namespace tpl::ast {

namespace {

/**
 * Visitor class that walks a type hierarchy tree with the purpose of pretty-printing to an injected
 * output stream.
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

void TypePrinter::VisitBuiltinType(const BuiltinType *type) { os() << type->GetTplName(); }

void TypePrinter::VisitFunctionType(const FunctionType *type) {
  os() << "(";
  bool first = true;
  for (const auto &param : type->GetParams()) {
    if (!first) {
      os() << ",";
    }
    first = false;
    Visit(param.type);
  }
  os() << ")->";
  Visit(type->GetReturnType());
}

void TypePrinter::VisitStringType(const StringType *type) { os() << "string"; }

void TypePrinter::VisitPointerType(const PointerType *type) {
  os() << "*";
  Visit(type->GetBase());
}

void TypePrinter::VisitStructType(const StructType *type) {
  os() << "struct{";
  bool first = true;
  for (const auto &field : type->GetFields()) {
    if (!first) {
      os() << ",";
    }
    first = false;
    Visit(field.type);
  }
  os() << "}";
}

void TypePrinter::VisitArrayType(const ArrayType *type) {
  os() << "[";
  if (type->HasUnknownLength()) {
    os() << "*";
  } else {
    os() << type->GetLength();
  }
  os() << "]";
  Visit(type->GetElementType());
}

void TypePrinter::VisitMapType(const MapType *type) {
  os() << "map[";
  Visit(type->GetKeyType());
  os() << "]";
  Visit(type->GetValueType());
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

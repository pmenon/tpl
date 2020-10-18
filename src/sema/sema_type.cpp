#include "sema/sema.h"

#include <utility>

#include "ast/context.h"
#include "ast/type.h"

namespace tpl::sema {

void Sema::VisitArrayTypeRepr(ast::ArrayTypeRepr *node) {
  uint64_t arr_len = 0;
  if (node->Length() != nullptr) {
    if (!node->Length()->IsIntegerLiteral()) {
      error_reporter()->Report(node->Length()->Position(), ErrorMessages::kNonIntegerArrayLength);
      return;
    }

    auto length = node->Length()->As<ast::LiteralExpr>()->Int32Val();
    if (length < 0) {
      error_reporter()->Report(node->Length()->Position(), ErrorMessages::kNegativeArrayLength);
      return;
    }

    arr_len = static_cast<uint64_t>(length);
  }

  ast::Type *elem_type = Resolve(node->ElementType());

  if (elem_type == nullptr) {
    return;
  }

  node->SetType(ast::ArrayType::Get(arr_len, elem_type));
}

void Sema::VisitFunctionTypeRepr(ast::FunctionTypeRepr *node) {
  // Handle parameters
  util::RegionVector<ast::Field> param_types(context()->GetRegion());
  for (auto *param : node->Parameters()) {
    Visit(param);
    ast::Type *param_type = param->TypeRepr()->GetType();
    if (param_type == nullptr) {
      return;
    }
    param_types.emplace_back(param->Name(), param_type);
  }

  // Handle return type
  ast::Type *ret = Resolve(node->ReturnType());
  if (ret == nullptr) {
    return;
  }

  // Create type
  ast::FunctionType *func_type = ast::FunctionType::Get(std::move(param_types), ret);
  node->SetType(func_type);
}

void Sema::VisitPointerTypeRepr(ast::PointerTypeRepr *node) {
  ast::Type *base_type = Resolve(node->Base());
  if (base_type == nullptr) {
    return;
  }

  node->SetType(base_type->PointerTo());
}

void Sema::VisitStructTypeRepr(ast::StructTypeRepr *node) {
  util::RegionVector<ast::Field> field_types(context()->GetRegion());
  for (auto *field : node->Fields()) {
    Visit(field);
    ast::Type *field_type = field->TypeRepr()->GetType();
    if (field_type == nullptr) {
      return;
    }
    field_types.emplace_back(field->Name(), field_type);
  }

  node->SetType(ast::StructType::Get(context(), std::move(field_types)));
}

void Sema::VisitMapTypeRepr(ast::MapTypeRepr *node) {
  ast::Type *key_type = Resolve(node->KeyType());
  ast::Type *value_type = Resolve(node->ValType());

  if (key_type == nullptr || value_type == nullptr) {
    // Error
    return;
  }

  node->SetType(ast::MapType::Get(key_type, value_type));
}

}  // namespace tpl::sema

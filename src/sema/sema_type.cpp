#include "sema/sema.h"

#include <utility>

#include "ast/context.h"
#include "ast/type.h"
#include "sema/error_reporter.h"

namespace tpl::sema {

void Sema::VisitArrayTypeRepr(ast::ArrayTypeRepr *node) {
  uint64_t arr_len = 0;
  if (node->GetLength() != nullptr) {
    if (!node->GetLength()->IsIntegerLiteral()) {
      error_reporter_->Report(node->GetLength()->Position(), ErrorMessages::kNonIntegerArrayLength);
      return;
    }

    auto length = node->GetLength()->As<ast::LiteralExpr>()->IntegerVal();
    if (length < 0) {
      error_reporter_->Report(node->GetLength()->Position(), ErrorMessages::kNegativeArrayLength);
      return;
    }

    arr_len = static_cast<uint64_t>(length);
  }

  ast::Type *elem_type = Resolve(node->GetElementType());

  if (elem_type == nullptr) {
    return;
  }

  node->SetType(ast::ArrayType::Get(arr_len, elem_type));
}

void Sema::VisitFunctionTypeRepr(ast::FunctionTypeRepr *node) {
  // Handle parameters
  util::RegionVector<ast::Field> param_types(context_->GetRegion());
  for (auto *param : node->GetParameters()) {
    Visit(param);
    ast::Type *param_type = param->GetTypeRepr()->GetType();
    if (param_type == nullptr) {
      return;
    }
    param_types.emplace_back(param->GetName(), param_type);
  }

  // Handle return type
  ast::Type *ret = Resolve(node->GetReturnType());
  if (ret == nullptr) {
    return;
  }

  // Create type
  ast::FunctionType *func_type = ast::FunctionType::Get(std::move(param_types), ret);
  node->SetType(func_type);
}

void Sema::VisitPointerTypeRepr(ast::PointerTypeRepr *node) {
  ast::Type *base_type = Resolve(node->GetBase());
  if (base_type == nullptr) {
    return;
  }

  node->SetType(base_type->PointerTo());
}

void Sema::VisitStructTypeRepr(ast::StructTypeRepr *node) {
  util::RegionVector<ast::Field> field_types(context_->GetRegion());
  for (auto *field : node->GetFields()) {
    Visit(field);
    ast::Type *field_type = field->GetTypeRepr()->GetType();
    if (field_type == nullptr) {
      return;
    }
    field_types.emplace_back(field->GetName(), field_type);
  }

  node->SetType(ast::StructType::Get(context_, std::move(field_types)));
}

void Sema::VisitMapTypeRepr(ast::MapTypeRepr *node) {
  ast::Type *key_type = Resolve(node->GetKeyType());
  ast::Type *value_type = Resolve(node->GetValueType());

  if (key_type == nullptr || value_type == nullptr) {
    // Error
    return;
  }

  node->SetType(ast::MapType::Get(key_type, value_type));
}

}  // namespace tpl::sema

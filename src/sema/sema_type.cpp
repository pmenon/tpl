#include "sema/sema.h"

#include <utility>

#include "ast/context.h"
#include "ast/type.h"
#include "logging/logger.h"
#include "sema/error_reporter.h"

namespace tpl::sema {

void Sema::VisitArrayTypeRepr(ast::ArrayTypeRepr *node) {
  // We use array-length of 0 to indicate an unsized array. We check the size if
  // one was provided in source.
  uint64_t arr_len = 0;
  if (node->GetLength() != nullptr) {
    if (!node->GetLength()->IsIntegerLiteral()) {
      error_reporter_->Report(node->GetLength()->Position(), ErrorMessages::kNonIntegerArrayLength);
      return;
    }

    const auto length = node->GetLength()->As<ast::LiteralExpression>()->IntegerVal();
    if (length < 0) {
      error_reporter_->Report(node->GetLength()->Position(), ErrorMessages::kNegativeArrayLength);
      return;
    }

    arr_len = static_cast<uint64_t>(length);
  }

  auto elem_type = Resolve(node->GetElementType());
  if (elem_type == nullptr) {
    return;
  }

  auto array_type = ast::ArrayType::Get(arr_len, elem_type);
  node->SetType(array_type);
}

void Sema::VisitFunctionTypeRepr(ast::FunctionTypeRepr *node) {
  util::RegionVector<ast::Field> param_types(context_->GetRegion());
  param_types.reserve(node->NumParameters());

  for (auto *param : node->GetParameters()) {
    Visit(param);
    auto param_type = param->GetTypeRepr()->GetType();

    // We quit early as soon we're unable to resolve a field.
    // TODO(pmenon): Should we continue, but fail in the end?
    if (param_type == nullptr) {
      return;
    }

    param_types.emplace_back(param->GetName(), param_type);
  }

  // Handle return type.
  auto ret = Resolve(node->GetReturnType());
  if (ret == nullptr) {
    return;
  }

  // Create type.
  auto func_type = ast::FunctionType::Get(std::move(param_types), ret);
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
  // Collect the resolved struct member types in this vector.
  util::RegionVector<ast::Field> field_types(context_->GetRegion());
  field_types.reserve(node->NumFields());

  for (auto *field : node->GetFields()) {
    Visit(field);
    ast::Type *field_type = field->GetTypeRepr()->GetType();

    // We quit early as soon we're unable to resolve a field.
    // TODO(pmenon): Should we continue, but fail in the end?
    if (field_type == nullptr) {
      return;
    }

    field_types.emplace_back(field->GetName(), field_type);
  }

  // If this representation is for an outstanding struct declaration, use the
  // declaration's name to construct a named struct. Otherwise, it's an unnamed
  // (i.e., anonymous) struct.
  auto *decl_context = GetCurrentDeclarationContext();
  if (ast::StructDeclaration * struct_decl;
      decl_context != nullptr && (struct_decl = decl_context->SafeAs<ast::StructDeclaration>())) {
    node->SetType(ast::StructType::Get(context_, struct_decl->GetName(), std::move(field_types)));
  } else {
    node->SetType(ast::StructType::Get(context_, std::move(field_types)));
  }
}

void Sema::VisitMapTypeRepr(ast::MapTypeRepr *node) {
  ast::Type *key_type = Resolve(node->GetKeyType());
  ast::Type *value_type = Resolve(node->GetValueType());

  // If we are not able to resolve either the key- or value-type, we return
  // early to indicate an error.
  if (key_type == nullptr || value_type == nullptr) {
    return;
  }

  node->SetType(ast::MapType::Get(key_type, value_type));
}

}  // namespace tpl::sema

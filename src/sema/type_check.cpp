#include "sema/type_check.h"

namespace tpl::sema {

TypeChecker::TypeChecker(ast::AstContext &ctx)
    : ctx_(ctx),
      region_(ctx.region()),
      error_reporter_(ctx.error_reporter()),
      scope_(nullptr),
      curr_func_(nullptr) {}

bool TypeChecker::Run(ast::AstNode *root) {
  Visit(root);
  return error_reporter().has_errors();
}

void TypeChecker::VisitBadExpression(ast::BadExpression *node) {
  TPL_ASSERT(false);
}

void TypeChecker::VisitUnaryExpression(ast::UnaryExpression *node) {
  // Resolve the type of the sub expression
  ast::Type *expr_type = Resolve(node->expr());

  if (expr_type == nullptr) {
    return;
  }

  switch (node->op()) {
    case parsing::Token::BANG: {
      if (expr_type->IsBoolType()) {
        node->set_type(expr_type);
      } else {
        error_reporter().Report(node->position(),
                                ErrorMessages::kInvalidOperation, node->op(),
                                expr_type);
      }
      break;
    }
    case parsing::Token::MINUS: {
      if (expr_type->IsNumber()) {
        node->set_type(expr_type);
      } else {
        error_reporter().Report(node->position(),
                                ErrorMessages::kInvalidOperation, node->op(),
                                expr_type);
      }
      break;
    }
    case parsing::Token::Type::STAR: {
      if (auto *ptr_type = expr_type->SafeAs<ast::PointerType>()) {
        node->set_type(ptr_type->base());
      } else {
        error_reporter().Report(node->position(),
                                ErrorMessages::kInvalidOperation, node->op(),
                                expr_type);
      }
      break;
    }
    case parsing::Token::Type::AMPERSAND: {
      node->set_type(expr_type->PointerTo());
      break;
    }
    default: {}
  }
}

void TypeChecker::VisitAssignmentStatement(ast::AssignmentStatement *node) {
  auto *src_type = Resolve(node->source());
  auto *dest_type = Resolve(node->destination());

  if (src_type == nullptr || dest_type == nullptr) {
    // Skip
  }

  if (src_type != dest_type) {
    // Error
  }
}

void TypeChecker::VisitBlockStatement(ast::BlockStatement *node) {
  auto *block_scope = OpenScope(Scope::Kind::Block);

  for (auto *stmt : node->statements()) {
    Visit(stmt);
  }

  CloseScope(block_scope);
}

void TypeChecker::VisitFile(ast::File *node) {
  auto *file_scope = OpenScope(Scope::Kind::File);

  for (auto *decl : node->declarations()) {
    Visit(decl);
  }

  CloseScope(file_scope);
}

void TypeChecker::VisitVariableDeclaration(ast::VariableDeclaration *node) {
  if (scope()->LookupLocal(node->name()) != nullptr) {
    error_reporter().Report(node->position(),
                            ErrorMessages::kVariableRedeclared, node->name());
    return;
  }

  // At this point, the variable either has a declared type or an initial value
  TPL_ASSERT(node->type_repr() != nullptr || node->initial() != nullptr);

  ast::Type *declared_type = nullptr;
  ast::Type *initializer_type = nullptr;

  if (node->type_repr() != nullptr) {
    declared_type = Resolve(node->type_repr());
  }

  if (node->initial() != nullptr) {
    initializer_type = Resolve(node->initial());
  }

  if (declared_type == nullptr && initializer_type == nullptr) {
    // Error
    return;
  }

  if (declared_type != nullptr && initializer_type != nullptr) {
    // Check compatibility
  }

  // The type should be resolved now
  scope()->Declare(node->name(), (declared_type != nullptr ? declared_type
                                                           : initializer_type));
}

void TypeChecker::VisitFunctionDeclaration(ast::FunctionDeclaration *node) {
  auto *func_type = Resolve(node->function());
  if (func_type == nullptr) {
    return;
  }

  scope()->Declare(node->name(), func_type);
}

void TypeChecker::VisitStructDeclaration(ast::StructDeclaration *node) {
  auto *struct_type = Resolve(node->type_repr());
  if (struct_type == nullptr) {
    return;
  }

  scope()->Declare(node->name(), struct_type);
}

void TypeChecker::VisitIdentifierExpression(ast::IdentifierExpression *node) {
  auto *type = scope()->Lookup(node->name());

  if (type == nullptr) {
    type = ast_context().LookupBuiltin(node->name());
    if (type == nullptr) {
      error_reporter().Report(node->position(),
                              ErrorMessages::kUndefinedVariable, node->name());
      return;
    }
  }

  node->set_type(type);
}

void TypeChecker::VisitCallExpression(ast::CallExpression *node) {
  // Resolve the function type
  ast::Type *type = Resolve(node->function());

  if (type == nullptr) {
    return;
  }

  if (!type->IsFunctionType()) {
    error_reporter().Report(node->position(), ErrorMessages::kNonFunction);
    return;
  }

  ast::Identifier func_name =
      node->function()->As<ast::IdentifierExpression>()->name();

  // First, check to make sure we have the right number of function arguments
  auto *func_type = type->As<ast::FunctionType>();
  if (node->arguments().size() < func_type->params().size()) {
    error_reporter().Report(node->position(), ErrorMessages::kNotEnoughCallArgs,
                            func_name);
    return;
  } else if (node->arguments().size() > func_type->params().size()) {
    error_reporter().Report(node->position(), ErrorMessages::kTooManyCallArgs,
                            func_name);
    return;
  }

  // Now, let's resolve each function argument's type
  for (auto *arg : node->arguments()) {
    ast::Type *arg_type = Resolve(arg);
    if (arg_type == nullptr) {
      return;
    }
  }

  // Now, let's make sure the arguments match up
  const auto &arg_types = node->arguments();
  const auto &func_param_types = func_type->params();
  for (size_t i = 0; i < arg_types.size(); i++) {
    if (arg_types[i]->type() != func_param_types[i]) {
      // TODO(pmenon): Fix this check
      error_reporter().Report(
          node->position(), ErrorMessages::kIncorrectCallArgType,
          arg_types[i]->type(), func_param_types[i], func_name);
      return;
    }
  }

  // All looks good ...
  node->set_type(func_type->return_type());
}

void TypeChecker::VisitPointerTypeRepr(ast::PointerTypeRepr *node) {
  ast::Type *base_type = Resolve(node->base());
  if (base_type == nullptr) {
    return;
  }

  node->set_type(base_type->PointerTo());
}

void TypeChecker::VisitLiteralExpression(ast::LiteralExpression *node) {
  switch (node->literal_kind()) {
    case ast::LiteralExpression::LitKind::Nil: {
      node->set_type(ast::NilType::Nil(ast_context()));
      break;
    }
    case ast::LiteralExpression::LitKind::Boolean: {
      node->set_type(ast::BoolType::Bool(ast_context()));
      break;
    }
    case ast::LiteralExpression::LitKind::Float: {
      // Literal floats default to float32
      node->set_type(ast::FloatType::Float32(ast_context()));
      break;
    }
    case ast::LiteralExpression::LitKind::Int: {
      // Literal integers default to int32
      node->set_type(ast::IntegerType::Int32(ast_context()));
      break;
    }
    default: { TPL_ASSERT(false && "String literals not supported yet"); }
  }
}

void TypeChecker::VisitForStatement(ast::ForStatement *node) {
  // Create a new scope for variables introduced in initialization block
  auto *loop_scope = OpenScope(Scope::Kind::Loop);

  if (node->init() != nullptr) {
    Visit(node->init());
  }

  if (node->cond() != nullptr) {
    ast::Type *cond_type = Resolve(node->cond());
    if (!cond_type->IsBoolType()) {
      error_reporter().Report(node->cond()->position(),
                              ErrorMessages::kNonBoolForCondition);
    }
  }

  if (node->next() != nullptr) {
    Visit(node->next());
  }

  // The body
  Visit(node->body());

  // Close scope
  CloseScope(loop_scope);
}

void TypeChecker::VisitExpressionStatement(ast::ExpressionStatement *node) {
  Visit(node->expression());
}

void TypeChecker::VisitBadStatement(ast::BadStatement *node) {
  TPL_ASSERT(false);
}

void TypeChecker::VisitStructTypeRepr(ast::StructTypeRepr *node) {
  util::RegionVector<ast::Type *> elems(region());
  for (auto *elem : node->fields()) {
    auto *field_type = Resolve(elem->type_repr());
    if (field_type == nullptr) {
      // Error
      return;
    }
    elems.push_back(field_type);
  }

  node->set_type(ast::StructType::Get(ast_context(), std::move(elems)));
}

void TypeChecker::VisitIfStatement(ast::IfStatement *node) {
  ast::Type *cond_type = Resolve(node->cond());

  if (cond_type == nullptr) {
    // Error
    return;
  }

  if (!cond_type->IsBoolType()) {
    error_reporter().Report(node->cond()->position(),
                            ErrorMessages::kNonBoolIfCondition);
  }

  Visit(node->then_stmt());

  if (node->else_stmt() != nullptr) {
    Visit(node->else_stmt());
  }
}

void TypeChecker::VisitDeclarationStatement(ast::DeclarationStatement *node) {
  Visit(node->declaration());
}

void TypeChecker::VisitArrayTypeRepr(ast::ArrayTypeRepr *node) {
  uint64_t actual_length = 0;
  if (node->length() != nullptr) {
    auto *len_expr = node->length()->SafeAs<ast::LiteralExpression>();
    if (len_expr == nullptr ||
        len_expr->literal_kind() != ast::LiteralExpression::LitKind::Int) {
      error_reporter().Report(node->length()->position(),
                              ErrorMessages::kNonIntegerArrayLength);
      return;
    }

    auto len = len_expr->integer();
    if (len < 0) {
      error_reporter().Report(node->length()->position(),
                              ErrorMessages::kNegativeArrayLength);
      return;
    }

    actual_length = static_cast<uint64_t>(len);
  }

  ast::Type *elem_type = Resolve(node->element_type());

  if (elem_type == nullptr) {
    return;
  }

  node->set_type(ast::ArrayType::Get(actual_length, elem_type));
}

void TypeChecker::VisitBinaryExpression(ast::BinaryExpression *node) {
  ast::Type *left_type = Resolve(node->left());
  ast::Type *right_type = Resolve(node->right());

  // TODO(pmenon): Fix me
  TPL_ASSERT(left_type == right_type);
}

void TypeChecker::VisitFunctionLiteralExpression(
    ast::FunctionLiteralExpression *node) {
  // Resolve the type
  ast::Type *type = Resolve(node->type_repr());
  if (type == nullptr) {
    return;
  }

  // Good function type, insert into node
  auto *func_type = type->As<ast::FunctionType>();
  node->set_type(type->As<ast::FunctionType>());

  // Start a new function scope
  auto *function_scope = OpenScope(Scope::Kind::Function);

  FunctionScope scoped(*this, node);

  // Declare function parameters in scope
  const auto &repr_params = node->type_repr()->parameters();
  const auto &param_types = func_type->params();
  for (size_t i = 0; i < func_type->params().size(); i++) {
    scope()->Declare(repr_params[i]->name(), param_types[i]);
  }

  // Recurse into the function body
  Visit(node->body());

  // Close up the function scope
  CloseScope(function_scope);
}

void TypeChecker::VisitReturnStatement(ast::ReturnStatement *node) {
  if (current_function() == nullptr) {
    error_reporter().Report(node->position(),
                            ErrorMessages::kReturnOutsideFunction);
    return;
  }

  ast::Type *ret = Resolve(node->ret());
  if (ret == nullptr) {
    return;
  }

  // Check return type matches function
  auto *func_type = current_function()->type()->As<ast::FunctionType>();
  if (ret != func_type->return_type()) {
    // Error
  }
}

void TypeChecker::VisitFunctionTypeRepr(ast::FunctionTypeRepr *node) {
  // Handle parameters
  util::RegionVector<ast::Type *> param_types(region());
  for (auto *param : node->parameters()) {
    auto *param_type = Resolve(param->type_repr());
    if (param_type == nullptr) {
      return;
    }
    param_types.push_back(param_type);
  }

  // Handle return type
  ast::Type *ret = Resolve(node->return_type());
  if (ret == nullptr) {
    return;
  }

  // Create type
  ast::FunctionType *func_type =
      ast::FunctionType::Get(std::move(param_types), ret);
  node->set_type(func_type);
}

}  // namespace tpl::sema
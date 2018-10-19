#include "vm/bytecode_generator.h"

#include "ast/type.h"
#include "logging/logger.h"
#include "sql/catalog.h"
#include "sql/table.h"
#include "util/macros.h"
#include "vm/bytecode_label.h"
#include "vm/bytecode_unit.h"
#include "vm/control_flow_builders.h"

namespace tpl::vm {

class BytecodeGenerator::ExpressionResultScope {
 public:
  ExpressionResultScope(BytecodeGenerator *generator, ast::Expr::Context kind,
                        LocalVar destination = LocalVar())
      : generator_(generator),
        outer_scope_(generator->execution_result()),
        destination_(destination),
        kind_(kind) {
    generator_->set_execution_result(this);
  }

  virtual ~ExpressionResultScope() {
    generator_->set_execution_result(outer_scope_);
  }

  bool IsLValue() const { return kind_ == ast::Expr::Context::LValue; }
  bool IsRValue() const { return kind_ == ast::Expr::Context::RValue; }
  bool IsEffect() const { return kind_ == ast::Expr::Context::Effect; }

  bool HasDestination() const { return !destination().IsInvalid(); }

  LocalVar GetOrCreateDestination(ast::Type *type) {
    if (!HasDestination()) {
      destination_ = generator_->current_function()->NewTempLocal(type);
    }

    return destination_;
  }

  LocalVar destination() const { return destination_; }
  void set_destination(LocalVar destination) { destination_ = destination; }

 private:
  BytecodeGenerator *generator_;
  ExpressionResultScope *outer_scope_;
  LocalVar destination_;
  ast::Expr::Context kind_;
};

class BytecodeGenerator::LValueResultScope
    : public BytecodeGenerator::ExpressionResultScope {
 public:
  LValueResultScope(BytecodeGenerator *generator, LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expr::Context::LValue, dest) {}
};

class BytecodeGenerator::RValueResultScope
    : public BytecodeGenerator::ExpressionResultScope {
 public:
  RValueResultScope(BytecodeGenerator *generator, LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expr::Context::RValue, dest) {}
};

class BytecodeGenerator::TestResultScope
    : public BytecodeGenerator::ExpressionResultScope {
 public:
  TestResultScope(BytecodeGenerator *generator, LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expr::Context::Test, dest) {}
};

/**
 * A handy scoped class that tracks the start and end positions in the bytecode
 * for a given function, automatically setting the range in the function upon
 * going out of scope.
 */
class BytecodeGenerator::BytecodePositionTracker {
 public:
  BytecodePositionTracker(BytecodeGenerator *generator, FunctionInfo *func)
      : generator_(generator),
        func_(func),
        start_offset_(generator->emitter()->position()) {}

  ~BytecodePositionTracker() {
    func_->MarkBytecodeRange(start_offset_, generator_->emitter()->position());
  }

 private:
  BytecodeGenerator *generator_;
  FunctionInfo *func_;
  std::size_t start_offset_;
};

BytecodeGenerator::BytecodeGenerator()
    : execution_result_(nullptr), func_id_counter_(0) {}

void BytecodeGenerator::VisitIfStmt(ast::IfStmt *node) {
  IfThenElseBuilder if_builder(this);

  // Generate condition check code
  VisitExpressionForTest(node->condition(), if_builder.then_label(),
                         if_builder.else_label(), TestFallthrough::Then);

  // Generate code in "then" block
  if_builder.Then();
  Visit(node->then_stmt());

  // If there's an "else" block, handle it now
  if (node->else_stmt() != nullptr) {
    if_builder.JumpToEnd();
    if_builder.Else();
    Visit(node->else_stmt());
  }
}

void BytecodeGenerator::VisitIterationStatement(ast::IterationStmt *iteration,
                                                LoopBuilder *loop_builder) {
  Visit(iteration->body());
  loop_builder->BindContinueTarget();
}

void BytecodeGenerator::VisitForStmt(ast::ForStmt *node) {
  LoopBuilder loop_builder(this);

  if (node->init() != nullptr) {
    Visit(node->init());
  }

  loop_builder.LoopHeader();

  if (node->condition() != nullptr) {
    BytecodeLabel loop_body_label;
    VisitExpressionForTest(node->condition(), &loop_body_label,
                           loop_builder.break_label(), TestFallthrough::Then);
  }

  VisitIterationStatement(node, &loop_builder);

  if (node->next() != nullptr) {
    Visit(node->next());
  }

  loop_builder.JumpToHeader();
}

void BytecodeGenerator::VisitForInStmt(ast::ForInStmt *node) {
  TPL_ASSERT(node->iter()->IsIdentifierExpr(),
             "Iterable of for-in must be an identifier to a table, collection "
             "or array/list literal");
  // Create the iterator variable
  ast::AstContext &ctx = node->target()->type()->context();
  ast::InternalType *iter_type = ast::InternalType::Get(
      ctx, ast::InternalType::InternalKind::SqlTableIterator);
  LocalVar iter = current_function()->NewLocal(iter_type, "iter");

  // Initialize the iterator
  sql::Table *table = sql::Catalog::instance()->LookupTableByName(
      node->iter()->As<ast::IdentifierExpr>()->name().data());
  TPL_ASSERT(table != nullptr, "Table does not exist!");
  emitter()->Emit(Bytecode::SqlTableIteratorInit, iter, table->id());

  // Create the row type and pull pointers to the columns
  auto *row_type = node->target()->type()->As<ast::StructType>();
  LocalVar row = current_function()->NewLocal(row_type, "row");

  const auto &fields = row_type->fields();
  for (u32 idx = 0, offset = 0; idx < fields.size(); idx++) {
    LocalVar col_ptr = current_function()->NewLocal(
        fields[idx].type->PointerTo(), fields[idx].name.data());
    emitter()->EmitLea(col_ptr, row, offset);
    offset += fields[idx].type->size();
  }

  {
    // Loop body
    LoopBuilder loop_builder(this);
    loop_builder.LoopHeader();

    LocalVar cond = current_function()->NewTempLocal(ast::BoolType::Bool(ctx));
    emitter()->Emit(Bytecode::SqlTableIteratorNext, cond, iter);
    emitter()->EmitConditionalJump(Bytecode::JumpIfFalse, cond.ValueOf(),
                                   loop_builder.break_label());

    VisitIterationStatement(node, &loop_builder);

    loop_builder.JumpToHeader();
  }

  // Cleanup
  emitter()->Emit(Bytecode::SqlTableIteratorClose, iter);
}

void BytecodeGenerator::VisitFieldDecl(ast::FieldDecl *node) {
  AstVisitor::VisitFieldDecl(node);
}

void BytecodeGenerator::VisitFunctionDecl(ast::FunctionDecl *node) {
  // Create function info object
  FunctionInfo *func_info = AllocateFunc(node->name().data());

  auto *func_type = node->type_repr()->type()->As<ast::FunctionType>();

  // Register return type
  func_info->NewLocal(func_type->return_type(), "ret");

  // Register parameters
  for (const auto &func_param : func_type->params()) {
    func_info->NewParameterLocal(func_param.type, func_param.name.data());
  }

  {
    // Visit the body of the function
    BytecodePositionTracker position_tracker(this, func_info);
    Visit(node->function());
  }
}

void BytecodeGenerator::VisitIdentifierExpr(ast::IdentifierExpr *node) {
  LocalVar local = current_function()->LookupLocal(node->name().data());

  if (execution_result()->IsRValue()) {
    local = local.ValueOf();
  }

  execution_result()->set_destination(local);
}

void BytecodeGenerator::VisitBlockStmt(ast::BlockStmt *node) {
  for (auto *stmt : node->statements()) {
    Visit(stmt);
  }
}

void BytecodeGenerator::VisitVariableDecl(ast::VariableDecl *node) {
  // Register a new local variable in the function. If the variable has an
  // explicit type specifier, prefer using that. Otherwise, use the type of the
  // initial value resolved after semantic analysis.
  ast::Type *type = nullptr;
  if (node->type_repr() != nullptr) {
    TPL_ASSERT(node->type_repr()->type() != nullptr,
               "Variable with explicit type declaration is missing resolved "
               "type at runtime!");
    type = node->type_repr()->type();
  } else {
    TPL_ASSERT(node->initial() != nullptr,
               "Variable without explicit type declaration is missing an "
               "initialization expression!");
    TPL_ASSERT(node->initial()->type() != nullptr,
               "Variable with initial value is missing resolved type");
    type = node->initial()->type();
  }

  // Register this variable in the function as a local
  LocalVar local = current_function()->NewLocal(type, node->name().data());

  // If there's an initializer, generate code for it now
  if (node->initial() != nullptr) {
    VisitExpressionForRValue(node->initial(), local);
  }
}

void BytecodeGenerator::VisitUnaryOpExpr(ast::UnaryOpExpr *node) {
  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());
  LocalVar input = VisitExpressionForRValue(node->expr());

  Bytecode bytecode;
  switch (node->op()) {
    case parsing::Token::Type::MINUS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Neg),
                                     node->type());
      break;
    }
    case parsing::Token::Type::BIT_NOT: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitNeg),
                                     node->type());
      break;
    }
    default: { UNREACHABLE("Impossible unary operation"); }
  }

  // Emit
  emitter()->EmitUnaryOp(bytecode, dest, input);

  // Mark where the result is
  execution_result()->set_destination(dest);
}

void BytecodeGenerator::VisitReturnStmt(ast::ReturnStmt *node) {
  if (node->ret() != nullptr) {
    VisitExpressionForRValue(node->ret(), current_function()->GetRVLocal());
  }
  emitter()->EmitReturn();
}

void BytecodeGenerator::VisitCallExpr(ast::CallExpr *node) {
  AstVisitor::VisitCallExpr(node);
}

void BytecodeGenerator::VisitAssignmentStmt(ast::AssignmentStmt *node) {
  LocalVar dest = VisitExpressionForLValue(node->destination());
  VisitExpressionForRValue(node->source(), dest);
}

void BytecodeGenerator::VisitFile(ast::File *node) {
  for (auto *decl : node->declarations()) {
    Visit(decl);
  }
}

void BytecodeGenerator::VisitLitExpr(ast::LitExpr *node) {
  TPL_ASSERT(execution_result()->IsRValue(),
             "Literal expressions cannot be R-Values!");

  LocalVar target = execution_result()->GetOrCreateDestination(node->type());

  switch (node->literal_kind()) {
    case ast::LitExpr::LitKind::Nil: {
      // Do nothing
      break;
    }
    case ast::LitExpr::LitKind::Boolean: {
      emitter()->EmitLoadImm1(target, node->bool_val());
      break;
    }
    case ast::LitExpr::LitKind::Int: {
      emitter()->EmitLoadImm4(target, node->int32_val());
      break;
    }
    default: {
      LOG_ERROR("Non-bool or non-integer literals not supported in bytecode");
      break;
    }
  }

  if (execution_result()->IsRValue()) {
    execution_result()->set_destination(target.ValueOf());
  }
}

void BytecodeGenerator::VisitStructDecl(ast::StructDecl *node) {
  // TODO
  // curr_func()->NewLocal(node->type_repr()->type(), node->name().data(),
  // false);
}

void BytecodeGenerator::VisitBooleanBinaryOpExpr(ast::BinaryOpExpr *node) {
  TPL_ASSERT(execution_result()->IsRValue(),
             "Binary expressions must be R-Values!");
  TPL_ASSERT(node->left()->type()->kind() == node->right()->type()->kind(),
             "Binary operation has mismatched left and right types");
  TPL_ASSERT(node->type()->IsBoolType(),
             "Boolean binary operation must be of type bool");

  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());

  // Execute left child
  VisitExpressionForRValue(node->left(), dest);

  Bytecode conditional_jump;
  BytecodeLabel fallthrough;

  switch(node->op()) {
    case parsing::Token::Type::OR: {
      conditional_jump = Bytecode::JumpIfTrue;
      break;
    }
    case parsing::Token::Type::AND: {
      conditional_jump = Bytecode::JumpIfFalse;
      break;
    }
    default: { UNREACHABLE("Impossible binary operation of bool type"); }
  }

  // Do a conditional jump
  emitter()->EmitConditionalJump(Bytecode::JumpIfTrue, dest, &fallthrough);

  // Execute the right child
  VisitExpressionForRValue(node->right(), dest);

  // Bind the label for fallthrough
  emitter()->Bind(&fallthrough);

  // Mark where the result is
  execution_result()->set_destination(dest.ValueOf());
}

void BytecodeGenerator::VisitBinaryOpExpr(ast::BinaryOpExpr *node) {
  TPL_ASSERT(execution_result()->IsRValue(),
             "Binary expressions must be R-Values!");
  TPL_ASSERT(node->left()->type()->kind() == node->right()->type()->kind(),
             "Binary operation has mismatched left and right types");

  if (node->type()->IsBoolType()) {
    return VisitBooleanBinaryOpExpr(node);
  }

  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());
  LocalVar left = VisitExpressionForRValue(node->left());
  LocalVar right = VisitExpressionForRValue(node->right());

  Bytecode bytecode;
  switch (node->op()) {
    case parsing::Token::Type::PLUS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Add),
                                     node->type());
      break;
    }
    case parsing::Token::Type::MINUS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Sub),
                                     node->type());
      break;
    }
    case parsing::Token::Type::STAR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Mul),
                                     node->type());
      break;
    }
    case parsing::Token::Type::SLASH: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Div),
                                     node->type());
      break;
    }
    case parsing::Token::Type::PERCENT: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Rem),
                                     node->type());
      break;
    }
    case parsing::Token::Type::AMPERSAND: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitAnd),
                                     node->type());
      break;
    }
    case parsing::Token::Type::BIT_OR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitOr),
                                     node->type());
      break;
    }
    case parsing::Token::Type::BIT_XOR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitXor),
                                     node->type());
      break;
    }
    default: { UNREACHABLE("Impossible binary operation"); }
  }

  // Emit
  emitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  execution_result()->set_destination(dest.ValueOf());
}

void BytecodeGenerator::VisitComparisonOpExpr(ast::ComparisonOpExpr *node) {
  TPL_ASSERT(execution_result()->IsRValue(),
             "Comparison expressions must be R-Values!");
  TPL_ASSERT(node->type()->IsBoolType(),
             "Comparison op is expected to be boolean");

  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());
  LocalVar left = VisitExpressionForRValue(node->left());
  LocalVar right = VisitExpressionForRValue(node->right());

  Bytecode bytecode;
  switch (node->op()) {
    case parsing::Token::Type::GREATER: {
      bytecode = GetIntTypedBytecode(
          GET_BASE_FOR_INT_TYPES(Bytecode::GreaterThan), node->left()->type());
      break;
    }
    case parsing::Token::Type::GREATER_EQUAL: {
      bytecode = GetIntTypedBytecode(
          GET_BASE_FOR_INT_TYPES(Bytecode::GreaterThanEqual),
          node->left()->type());
      break;
    }
    case parsing::Token::Type::EQUAL_EQUAL: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Equal),
                                     node->left()->type());
      break;
    }
    case parsing::Token::Type::LESS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::LessThan),
                                     node->left()->type());
      break;
    }
    case parsing::Token::Type::LESS_EQUAL: {
      bytecode =
          GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::LessThanEqual),
                              node->left()->type());
      break;
    }
    case parsing::Token::Type::BANG_EQUAL: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::NotEqual),
                                     node->left()->type());
      break;
    }
    default: { UNREACHABLE("Impossible binary operation"); }
  }

  // Emit
  emitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  execution_result()->set_destination(dest.ValueOf());
}

void BytecodeGenerator::VisitFunctionLitExpr(ast::FunctionLitExpr *node) {
  Visit(node->body());
}

void BytecodeGenerator::VisitSelectorExpr(ast::SelectorExpr *node) {
  LocalVar obj = VisitExpressionForLValue(node->object());

  ast::StructType *obj_type = nullptr;
  if (auto *ptr_type = node->object()->type()->SafeAs<ast::PointerType>()) {
    obj = obj.ValueOf();
    obj_type = ptr_type->base()->As<ast::StructType>();
  } else {
    obj_type = node->object()->type()->As<ast::StructType>();
  }

  auto *field_name = node->selector()->As<ast::IdentifierExpr>();

  u32 offset = obj_type->GetOffsetOfFieldByName(field_name->name());

  if (execution_result()->IsLValue()) {
    TPL_ASSERT(!execution_result()->HasDestination(),
               "L-Values produce their destination");
    if (offset == 0) {
      // No LEA needed
      if (node->object()->type()->IsPointerType()) {
        execution_result()->set_destination(obj.ValueOf());
      } else {
        execution_result()->set_destination(obj);
      }
      return;
    }

    // Need to LEA
    LocalVar dest =
        execution_result()->GetOrCreateDestination(node->type()->PointerTo());
    emitter()->EmitLea(dest, obj, offset);
    execution_result()->set_destination(dest.ValueOf());
    return;
  }

  // Need to load address and deref

  LocalVar elem_ptr;
  if (offset == 0) {
    if (node->object()->type()->IsPointerType()) {
      elem_ptr = obj.ValueOf();
    } else {
      elem_ptr = obj;
    }
  } else {
    elem_ptr = current_function()->NewTempLocal(node->type()->PointerTo());
    emitter()->EmitLea(elem_ptr, obj, offset);
    elem_ptr = elem_ptr.ValueOf();
  }

  // TODO: This Deref size should depend on type!
  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());
  emitter()->EmitUnaryOp(Bytecode::Deref4, dest, elem_ptr);
  execution_result()->set_destination(dest.ValueOf());
}

void BytecodeGenerator::VisitDeclStmt(ast::DeclStmt *node) {
  Visit(node->declaration());
}

void BytecodeGenerator::VisitExpressionStmt(ast::ExpressionStmt *node) {
  Visit(node->expression());
}

void BytecodeGenerator::VisitBadExpr(ast::BadExpr *node) {
  TPL_ASSERT(false, "Visiting bad expression during code generation!");
}

void BytecodeGenerator::VisitArrayTypeRepr(ast::ArrayTypeRepr *node) {
  TPL_ASSERT(false, "Should not visit type-representation nodes!");
}

void BytecodeGenerator::VisitFunctionTypeRepr(ast::FunctionTypeRepr *node) {
  TPL_ASSERT(false, "Should not visit type-representation nodes!");
}

void BytecodeGenerator::VisitPointerTypeRepr(ast::PointerTypeRepr *node) {
  TPL_ASSERT(false, "Should not visit type-representation nodes!");
}

void BytecodeGenerator::VisitStructTypeRepr(ast::StructTypeRepr *node) {
  TPL_ASSERT(false, "Should not visit type-representation nodes!");
}

FunctionInfo *BytecodeGenerator::AllocateFunc(const std::string &name) {
  functions_.emplace_back(++func_id_counter_, name);
  return &functions_.back();
}

LocalVar BytecodeGenerator::VisitExpressionForLValue(ast::Expr *expr) {
  LValueResultScope scope(this);
  Visit(expr);
  return scope.destination();
}

LocalVar BytecodeGenerator::VisitExpressionForRValue(ast::Expr *expr) {
  RValueResultScope scope(this);
  Visit(expr);
  return scope.destination();
}

void BytecodeGenerator::VisitExpressionForRValue(ast::Expr *expr,
                                                 LocalVar dest) {
  RValueResultScope scope(this, dest);
  Visit(expr);
}

void BytecodeGenerator::VisitExpressionForTest(ast::Expr *expr,
                                               BytecodeLabel *then_label,
                                               BytecodeLabel *else_label,
                                               TestFallthrough fallthrough) {
  // Evaluate the expression
  LocalVar cond = VisitExpressionForRValue(expr);

  switch (fallthrough) {
    case TestFallthrough::Then: {
      emitter()->EmitConditionalJump(Bytecode::JumpIfFalse, cond, else_label);
      break;
    }
    case TestFallthrough::Else: {
      emitter()->EmitConditionalJump(Bytecode::JumpIfTrue, cond, then_label);
      break;
    }
    case TestFallthrough::None: {
      emitter()->EmitConditionalJump(Bytecode::JumpIfFalse, cond, else_label);
      emitter()->EmitJump(Bytecode::Jump, then_label);
      break;
    }
  }
}

Bytecode BytecodeGenerator::GetIntTypedBytecode(Bytecode bytecode,
                                                ast::Type *type) {
  TPL_ASSERT(type->IsIntegerType(), "Type must be integer type");
  auto *int_type = type->SafeAs<ast::IntegerType>();
  auto int_kind = static_cast<u8>(int_type->int_kind());
  return Bytecodes::FromByte(Bytecodes::ToByte(bytecode) + int_kind);
}

// static
std::unique_ptr<BytecodeUnit> BytecodeGenerator::Compile(ast::AstNode *root) {
  BytecodeGenerator generator;
  generator.Visit(root);

  const auto &code = generator.emitter()->Finish();
  const auto &functions = generator.functions();
  return BytecodeUnit::Create(code, functions);
}

}  // namespace tpl::vm

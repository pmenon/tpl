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
                        BytecodeLabel *then_label, BytecodeLabel *else_label,
                        LocalVar destination = LocalVar())
      : generator_(generator),
        outer_scope_(generator->execution_result()),
        then_label_(then_label),
        else_label_(else_label),
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

  BytecodeLabel *then_label() { return then_label_; }
  BytecodeLabel *else_label() { return else_label_; }

  void set_then_label(BytecodeLabel *then_label) {
    then_label_ = then_label;
  }

  void set_else_label(BytecodeLabel *else_label) {
    else_label_ = else_label;
  }

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
  BytecodeLabel *then_label_;
  BytecodeLabel *else_label_;
  LocalVar destination_;
  ast::Expr::Context kind_;
};

class BytecodeGenerator::LValueResultScope
    : public BytecodeGenerator::ExpressionResultScope {
 public:
  LValueResultScope(BytecodeGenerator *generator, BytecodeLabel *then_label,
                    BytecodeLabel *else_label, LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expr::Context::LValue, then_label,
                              else_label, dest) {}
};

class BytecodeGenerator::RValueResultScope
    : public BytecodeGenerator::ExpressionResultScope {
 public:
  RValueResultScope(BytecodeGenerator *generator, BytecodeLabel *then_label,
                    BytecodeLabel *else_label, LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expr::Context::RValue, then_label,
                              else_label, dest) {}
};

class BytecodeGenerator::TestResultScope
    : public BytecodeGenerator::ExpressionResultScope {
 public:
  TestResultScope(BytecodeGenerator *generator, BytecodeLabel *then_label,
                  BytecodeLabel *else_label, LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expr::Context::Test, then_label,
                              else_label, dest) {}
};

class BytecodeGenerator::StatementContext {
 public:
  StatementContext(BytecodeGenerator *generator,
                   BytecodeLabel *next_label = nullptr,
                   BytecodeLabel *break_label = nullptr,
                   BytecodeLabel *continue_label = nullptr)
      : generator_(generator),
        old_context_(generator->statement_context()),
        next_label_(next_label),
        break_label_(break_label),
        continue_label_(continue_label) {
    generator_->set_statement_context(this);
  }

  ~StatementContext() {
    generator_->set_statement_context(old_context_);
  }

  BytecodeLabel *next_label() { return next_label_; }
  BytecodeLabel *break_label() { return break_label_; }
  BytecodeLabel *continue_label() { return continue_label_; }

 private:
  BytecodeGenerator *generator_;
  StatementContext *old_context_;
  BytecodeLabel *next_label_;
  BytecodeLabel *break_label_;
  BytecodeLabel *continue_label_;
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

  BytecodeLabel *then_label = if_builder.then_label();
  BytecodeLabel *else_label = (node->else_stmt())
      ? if_builder.else_label() : statement_context()->next_label();

  // Generate condition check code
  VisitExpressionForTest(node->condition(), then_label,
                         else_label, TestFallthrough::Then);

  // Generate code in "then" block
  if_builder.Then();
  Visit(node->then_stmt());

  // If there's an "else" block, handle it now
  if (node->else_stmt() != nullptr) {
    if_builder.JumpToEnd(/*end_label=*/statement_context()->next_label());
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

  auto *old_context = statement_context();

  if (node->init() != nullptr) {
    Visit(node->init());
  }

  loop_builder.LoopHeader();

  if (node->condition() != nullptr) {
    VisitExpressionForTest(node->condition(), loop_builder.body_label(),
                           old_context->next_label(), TestFallthrough::Then);
    loop_builder.LoopBody();
  }

  {
    BytecodeLabel *label = (node->next())
        ? loop_builder.continue_label() : loop_builder.header_label();
    StatementContext new_context(
        this,
        /*next_label=*/label,
        /*break_label=*/old_context->next_label(),
        /*continue_label=*/label);

    VisitIterationStatement(node, &loop_builder);
  }

  if (node->next() != nullptr) {
    // TODO(Siva): next statement cannot be complicated. So maybe, break label
    // and continue label is not needed.
    StatementContext new_context(
        this,
        /*next_label=*/loop_builder.header_label(),
        /*break_label=*/old_context->next_label(),
        /*continue_label=*/loop_builder.header_label());
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

  // Create the row type
  auto *row_type = node->target()->type()->As<ast::StructType>();
  LocalVar row = current_function()->NewLocal(row_type, "row");

  {
    auto *old_context = statement_context();

    // Loop body
    LoopBuilder loop_builder(this);
    loop_builder.LoopHeader();

    LocalVar cond = current_function()->NewTempLocal(ast::BoolType::Get(ctx));
    emitter()->Emit(Bytecode::SqlTableIteratorNext, cond, iter);
    emitter()->EmitConditionalJump(Bytecode::JumpIfFalse, cond.ValueOf(),
                                   old_context->next_label());

    loop_builder.LoopBody();

    StatementContext new_context(
        this,
        /*next_label=*/loop_builder.header_label(),
        /*break_label=*/old_context->next_label(),
        /*continue_label=*/loop_builder.header_label());

    // Load fields
    const auto &fields = row_type->fields();
    for (u32 col_idx = 0, offset = 0; col_idx < fields.size(); col_idx++) {
      LocalVar col_ptr =
          current_function()->NewTempLocal(fields[col_idx].type->PointerTo());
      emitter()->EmitLea(col_ptr, row, offset);
      emitter()->EmitRead(Bytecode::ReadInteger, iter, col_idx,
                          col_ptr.ValueOf());
      offset += fields[col_idx].type->size();
    }

    // Generate body
    VisitIterationStatement(node, &loop_builder);

    // Finish, loop back around
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
    BytecodeLabel end_label;
    StatementContext new_context(this, &end_label, &end_label, &end_label);
    BytecodePositionTracker position_tracker(this, func_info);
    Visit(node->function());
    emitter()->Bind(&end_label);
  }
}

void BytecodeGenerator::VisitIdentifierExpr(ast::IdentifierExpr *node) {
  /*
   * Lookup the local in the current function. It must be there through a
   * previous variable declaration (or parameter declaration). What is returned
   * is a pointer to the variable.
   */

  LocalVar local = current_function()->LookupLocal(node->name().data());

  if (execution_result()->IsLValue()) {
    execution_result()->set_destination(local);
    return;
  }

  /*
   * The caller wants the R-Value of the identifier. So, we need to load it. If
   * the caller did not provide a destination register, we're done. If the
   * caller provided a destination, we need to move the value of the identifier
   * into the provided destination.
   */

  local = local.ValueOf();

  if (!execution_result()->HasDestination()) {
    execution_result()->set_destination(local);
    return;
  }

  // We need to move the value of the variable into the provided destination
  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());
  emitter()->Emit(
      GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Move), node->type()),
      dest, local);
  execution_result()->set_destination(dest);
}

void BytecodeGenerator::VisitImplicitCastExpr(ast::ImplicitCastExpr *node) {
  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());
  LocalVar input = VisitExpressionForRValue(node->input());

  switch (node->cast_kind()) {
    case ast::ImplicitCastExpr::CastKind::SqlBoolToBool: {
      emitter()->Emit(Bytecode::ForceBoolTruth, dest, input);
      execution_result()->set_destination(dest.ValueOf());
      break;
    }
    case ast::ImplicitCastExpr::CastKind::IntToSqlInt: {
      emitter()->Emit(Bytecode::InitInteger, dest, input);
      execution_result()->set_destination(dest);
      break;
    }
    default: {
      // Implement me
      throw std::runtime_error("Implement me");
    }
  }
}

void BytecodeGenerator::VisitIndexExpr(ast::IndexExpr *node) {
#if 0
  LocalVar arr = VisitExpressionForLValue(node->object());

  ast::ArrayType *type = node->object()->type()->As<ast::ArrayType>();
  u32 elem_size = type->element_type()->size();

  LocalVar idx = VisitExpressionForRValue(node->index());

  LocalVar elem_ptr =
      current_function()->NewTempLocal(node->type()->PointerTo());
#endif
}

void BytecodeGenerator::VisitBlockStmt(ast::BlockStmt *node) {
  auto *old_context = statement_context();
  const auto &statements = node->statements();
  for (auto i = 0; i < (statements.size() - 1); i++) {
    auto *stmt = statements[i];
    BytecodeLabel next_label;
    StatementContext new_context(
        this,
        /*next_label=*/&next_label,
        /*break_label=*/old_context->break_label(),
        /*continue_label=*/old_context->continue_label());
    Visit(stmt);
    emitter()->Bind(&next_label);
  }

  // Visit the last statement
  auto *stmt = statements[statements.size() - 1];
  StatementContext new_context(
      this,
      /*next_label=*/old_context->next_label(),
      /*break_label=*/old_context->break_label(),
      /*continue_label=*/old_context->continue_label());
  Visit(stmt);
}

void BytecodeGenerator::VisitBreakStmt(ast::BreakStmt *node) {
  emitter()->EmitJump(statement_context()->break_label());
}

void BytecodeGenerator::VisitContinueStmt(ast::ContinueStmt *node) {
  emitter()->EmitJump(statement_context()->continue_label());
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
  if (node->destination()->type()->IsBoolType()) {
    LocalVar src = VisitExpressionForRValue(node->source());
    emitter()->Emit(GET_BASE_FOR_BOOL_TYPES(Bytecode::Move), dest, src);
  } else {
    VisitExpressionForRValue(node->source(), dest);
  }
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

void BytecodeGenerator::VisitLogicalAndOrExpr(ast::BinaryOpExpr *node) {
  TPL_ASSERT(execution_result()->IsRValue(),
             "Binary expressions must be R-Values!");
  TPL_ASSERT(node->left()->type()->kind() == node->right()->type()->kind(),
             "Binary operation has mismatched left and right types");
  TPL_ASSERT(node->type()->IsBoolType(),
             "Boolean binary operation must be of type bool");

  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());

  BytecodeLabel then_label, else_label;
  if (!execution_result()->then_label()) {
    execution_result()->set_then_label(&then_label);
  }
  if (!execution_result()->else_label()) {
    execution_result()->set_else_label(&else_label);
  }

  BytecodeLabel next_label;

  switch (node->op()) {
    case parsing::Token::Type::OR: {
      VisitExpressionForTest(node->left(),
                         /*then_label=*/execution_result()->then_label(),
                         /*else_label=*/&next_label,
                         TestFallthrough::Else,
                         dest);
      break;
    }
    case parsing::Token::Type::AND: {
      VisitExpressionForTest(node->left(),
                         /*then_label=*/&next_label,
                         /*else_label=*/execution_result()->else_label(),
                         TestFallthrough::Then,
                         dest);
      break;
    }
    default: { UNREACHABLE("Impossible logical operation type"); }
  }

  emitter()->Bind(&next_label);

  // Execute the right child
  VisitExpressionForRValue(node->right(), dest,
                           /*then_label=*/execution_result()->then_label(),
                           /*else_label=*/execution_result()->else_label());

  emitter()->Bind(&then_label);
  emitter()->Bind(&else_label);

  // Mark where the result is
  execution_result()->set_destination(dest.ValueOf());
}

void BytecodeGenerator::VisitArithmeticExpr(ast::BinaryOpExpr *node) {
  TPL_ASSERT(execution_result()->IsRValue(),
             "Arithmetic expressions must be R-Values!");
  TPL_ASSERT(node->left()->type()->kind() == node->right()->type()->kind(),
             "Arithmetic operation has mismatched left and right types");

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

void BytecodeGenerator::VisitBinaryOpExpr(ast::BinaryOpExpr *node) {
  switch (node->op()) {
    case parsing::Token::Type::AND:
    case parsing::Token::Type::OR: {
      VisitLogicalAndOrExpr(node);
      break;
    }
    default: {
      VisitArithmeticExpr(node);
      break;
    }
  }
}

void BytecodeGenerator::VisitSqlCompareOpExpr(ast::ComparisonOpExpr *compare) {
  TPL_ASSERT(execution_result()->IsRValue(),
             "SQL comparison expressions must be R-Values!");

  LocalVar dest = execution_result()->GetOrCreateDestination(compare->type());
  LocalVar left = VisitExpressionForLValue(compare->left());
  LocalVar right = VisitExpressionForLValue(compare->right());

  Bytecode code;
  switch (compare->op()) {
    case parsing::Token::Type::GREATER: {
      code = Bytecode::GreaterThanInteger;
      break;
    }
    case parsing::Token::Type::GREATER_EQUAL: {
      code = Bytecode::GreaterThanEqualInteger;
      break;
    }
    case parsing::Token::Type::EQUAL_EQUAL: {
      code = Bytecode::EqualInteger;
      break;
    }
    case parsing::Token::Type::LESS: {
      code = Bytecode::LessThanInteger;
      break;
    }
    case parsing::Token::Type::LESS_EQUAL: {
      code = Bytecode::LessThanEqualInteger;
      break;
    }
    case parsing::Token::Type::BANG_EQUAL: {
      code = Bytecode::NotEqualInteger;
      break;
    }
    default: { UNREACHABLE("Impossible binary operation"); }
  }

  // Emit
  emitter()->EmitBinaryOp(code, dest, left, right);

  // Mark where the result is
  execution_result()->set_destination(dest);
}

void BytecodeGenerator::VisitPrimitiveCompareOpExpr(
    ast::ComparisonOpExpr *compare) {
  TPL_ASSERT(execution_result()->IsRValue(),
             "Comparison expressions must be R-Values!");

  LocalVar dest = execution_result()->GetOrCreateDestination(compare->type());
  LocalVar left = VisitExpressionForRValue(compare->left());
  LocalVar right = VisitExpressionForRValue(compare->right());

  Bytecode bytecode;
  switch (compare->op()) {
    case parsing::Token::Type::GREATER: {
      bytecode =
          GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::GreaterThan),
                              compare->left()->type());
      break;
    }
    case parsing::Token::Type::GREATER_EQUAL: {
      bytecode = GetIntTypedBytecode(
          GET_BASE_FOR_INT_TYPES(Bytecode::GreaterThanEqual),
          compare->left()->type());
      break;
    }
    case parsing::Token::Type::EQUAL_EQUAL: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Equal),
                                     compare->left()->type());
      break;
    }
    case parsing::Token::Type::LESS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::LessThan),
                                     compare->left()->type());
      break;
    }
    case parsing::Token::Type::LESS_EQUAL: {
      bytecode =
          GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::LessThanEqual),
                              compare->left()->type());
      break;
    }
    case parsing::Token::Type::BANG_EQUAL: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::NotEqual),
                                     compare->left()->type());
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

  /*
   * We treat SQL comparisons slightly differently that primitive comparisons.
   */

  if (node->type()->IsSqlType()) {
    VisitSqlCompareOpExpr(node);
  } else {
    VisitPrimitiveCompareOpExpr(node);
  }
}

void BytecodeGenerator::VisitFunctionLitExpr(ast::FunctionLitExpr *node) {
  Visit(node->body());
}

LocalVar BytecodeGenerator::BuildLoadPointer(LocalVar double_ptr,
                                             ast::Type *type) {
  if (double_ptr.GetAddressMode() == LocalVar::AddressMode::Address) {
    return double_ptr.ValueOf();
  }

  // Need to Deref
  LocalVar ptr = current_function()->NewTempLocal(type);
  emitter()->EmitDeref<Bytecode::Deref8>(ptr, double_ptr);
  return ptr.ValueOf();
}

void BytecodeGenerator::VisitMemberExpr(ast::MemberExpr *node) {
  /*
   * We first need to compute the address of the object we're selecting into.
   * Thus, we get the L-Value of the object below.
   */

  LocalVar obj_ptr = VisitExpressionForLValue(node->object());

  /*
   * We now need to compute the offset of the field in the composite type. TPL
   * unifies C's arrow and dot syntax for field/member access. Thus, the type
   * of the object may be either a pointer to a struct or the actual struct. If
   * the type is a pointer, then the L-Value of the object is actually a double
   * pointer. Thus, we need to dereference it.
   */

  ast::StructType *obj_type = nullptr;
  if (auto *type = node->object()->type(); node->IsSugaredArrow()) {
    // Double pointer, need to dereference
    obj_ptr = BuildLoadPointer(obj_ptr, type);
    obj_type = type->As<ast::PointerType>()->base()->As<ast::StructType>();
  } else {
    obj_type = type->As<ast::StructType>();
  }

  /*
   * We're now ready to compute offset. Let's lookup the field's offset in the
   * struct type.
   */

  auto *field_name = node->member()->As<ast::IdentifierExpr>();
  auto offset = obj_type->GetOffsetOfFieldByName(field_name->name());

  /*
   * Now that we have a pointer to the composite object, we need to compute a
   * pointer to the field within the object. If the offset of the field in the
   * object is zero, we needn't do anything - we can just reinterpret the object
   * pointer. If the field offset is greater than zero, we generate a LEA.
   */

  LocalVar field_ptr;
  if (offset == 0) {
    field_ptr = obj_ptr;
  } else {
    field_ptr = current_function()->NewTempLocal(node->type()->PointerTo());
    emitter()->EmitLea(field_ptr, obj_ptr, offset);
    field_ptr = field_ptr.ValueOf();
  }

  if (execution_result()->IsLValue()) {
    TPL_ASSERT(!execution_result()->HasDestination(),
               "L-Values produce their destination");
    execution_result()->set_destination(field_ptr);
    return;
  }

  /*
   * The caller wants the actual value of the field. We just computed a pointer
   * to the field in the object, so we need to load/dereference it. If the
   * caller provided a destination variable, use that; otherwise, create a new
   * temporary variable to store the value.
   */

  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());

  // Emit the appropriate deref
  if (auto size = node->type()->size(); size == 1) {
    emitter()->EmitDeref<Bytecode::Deref1>(dest, field_ptr);
  } else if (size == 2) {
    emitter()->EmitDeref<Bytecode::Deref2>(dest, field_ptr);
  } else if (size == 4) {
    emitter()->EmitDeref<Bytecode::Deref4>(dest, field_ptr);
  } else if (size == 8) {
    emitter()->EmitDeref<Bytecode::Deref8>(dest, field_ptr);
  } else {
    emitter()->EmitDerefN(dest, field_ptr, size);
  }

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

void BytecodeGenerator::VisitMapTypeRepr(ast::MapTypeRepr *node) {
  TPL_ASSERT(false, "Should not visit type-representation nodes!");
}

FunctionInfo *BytecodeGenerator::AllocateFunc(const std::string &name) {
  functions_.emplace_back(++func_id_counter_, name);
  return &functions_.back();
}

LocalVar BytecodeGenerator::VisitExpressionForLValue(ast::Expr *expr) {
  LValueResultScope scope(this, nullptr, nullptr);
  Visit(expr);
  return scope.destination();
}

LocalVar BytecodeGenerator::VisitExpressionForRValue(
    ast::Expr *expr, BytecodeLabel *then_label, BytecodeLabel *else_label) {
  RValueResultScope scope(this, then_label, else_label);
  Visit(expr);
  return scope.destination();
}

void BytecodeGenerator::VisitExpressionForRValue(
    ast::Expr *expr, LocalVar dest, BytecodeLabel *then_label,
    BytecodeLabel *else_label) {
  RValueResultScope scope(this, then_label, else_label, dest);
  Visit(expr);
}

void BytecodeGenerator::VisitExpressionForTest(
    ast::Expr *expr, BytecodeLabel *then_label, BytecodeLabel *else_label,
    TestFallthrough fallthrough, LocalVar cond) {
  // Evaluate the expression
  if (cond.IsInvalid()) {
    cond = VisitExpressionForRValue(expr, then_label, else_label);
  } else {
    VisitExpressionForRValue(expr, cond, then_label, else_label);
    cond = cond.ValueOf();
  }

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
      emitter()->EmitJump(then_label);
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

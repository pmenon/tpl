#include "vm/bytecode_generator.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ast/builtins.h"
#include "ast/context.h"
#include "ast/type.h"
#include "common/macros.h"
#include "logging/logger.h"
#include "sql/catalog.h"
#include "sql/table.h"
#include "vm/bytecode_label.h"
#include "vm/bytecode_module.h"
#include "vm/control_flow_builders.h"

namespace tpl::vm {

/**
 * ExpressionResultScope is an RAII class that provides metadata about the usage of an expression
 * and its result. Callers construct one of its subclasses to let children nodes know the context in
 * which the expression's result is needed (i.e., whether the expression is an L-Value or R-Value).
 * It also tracks <b>where</b> the result of an expression is, somewhat emulating destination-driven
 * code generation.
 *
 * This is a base class for both LValue and RValue result scope objects.
 */
class BytecodeGenerator::ExpressionResultScope {
 public:
  /**
   * Construct an expression scope of kind @em kind. The destination where the result of the
   * expression is written to is @em destination.
   * @param generator The code generator.
   * @param kind The kind of expression.
   * @param destination Where the result of the expression is written to.
   */
  ExpressionResultScope(BytecodeGenerator *generator, ast::Expr::Context kind,
                        LocalVar destination = LocalVar())
      : generator_(generator),
        outer_scope_(generator->GetExecutionResult()),
        destination_(destination),
        kind_(kind) {
    generator_->SetExecutionResult(this);
  }

  /**
   * Destructor.
   */
  virtual ~ExpressionResultScope() { generator_->SetExecutionResult(outer_scope_); }

  /**
   * @return True if the expression is an L-Value expression; false otherwise.
   */
  bool IsLValue() const { return kind_ == ast::Expr::Context::LValue; }

  /**
   * @return True if the expression is an R-Value expression; false otherwise.
   */
  bool IsRValue() const { return kind_ == ast::Expr::Context::RValue; }

  /**
   * @return True if the expression has an assigned destination where the result is written to.
   */
  bool HasDestination() const { return !GetDestination().IsInvalid(); }

  /**
   * Return the destination where the result of the expression is written to. If one does not exist,
   * assign one of with type @em type and set it in this scope.
   * @param type The type of the result of the expression.
   * @return The destination where the result of the expression is written to.
   */
  LocalVar GetOrCreateDestination(ast::Type *type) {
    if (!HasDestination()) {
      destination_ = generator_->GetCurrentFunction()->NewLocal(type);
    }

    return destination_;
  }

  /**
   * @return The destination local where the result is written.
   */
  LocalVar GetDestination() const { return destination_; }

  /**
   * Set the local where the result of the expression is written to.
   */
  void SetDestination(LocalVar destination) { destination_ = destination; }

 private:
  BytecodeGenerator *generator_;
  ExpressionResultScope *outer_scope_;
  LocalVar destination_;
  ast::Expr::Context kind_;
};

/**
 * An expression result scope that indicates the result is used as an L-Value.
 */
class BytecodeGenerator::LValueResultScope : public BytecodeGenerator::ExpressionResultScope {
 public:
  explicit LValueResultScope(BytecodeGenerator *generator, LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expr::Context::LValue, dest) {}
};

/**
 * An expression result scope that indicates the result is used as an R-Value.
 */
class BytecodeGenerator::RValueResultScope : public BytecodeGenerator::ExpressionResultScope {
 public:
  explicit RValueResultScope(BytecodeGenerator *generator, LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expr::Context::RValue, dest) {}
};

/**
 * A handy scoped class that tracks the start and end positions in the bytecode for a given
 * function, automatically setting the range in the function upon going out of scope.
 */
class BytecodeGenerator::BytecodePositionScope {
 public:
  BytecodePositionScope(BytecodeGenerator *generator, FunctionInfo *func)
      : generator_(generator), func_(func), start_offset_(generator->GetEmitter()->GetPosition()) {}

  ~BytecodePositionScope() {
    const std::size_t end_offset = generator_->GetEmitter()->GetPosition();
    func_->set_bytecode_range(start_offset_, end_offset);
  }

 private:
  BytecodeGenerator *generator_;
  FunctionInfo *func_;
  std::size_t start_offset_;
};

// ---------------------------------------------------------
// Bytecode Generator begins
// ---------------------------------------------------------

BytecodeGenerator::BytecodeGenerator() noexcept : emitter_(&code_), execution_result_(nullptr) {}

void BytecodeGenerator::VisitIfStmt(ast::IfStmt *node) {
  IfThenElseBuilder if_builder(this);

  // Generate condition check code
  VisitExpressionForTest(node->condition(), if_builder.GetThenLabel(), if_builder.GetElseLabel(),
                         TestFallthrough::Then);

  // Generate code in "then" block
  if_builder.Then();
  Visit(node->then_stmt());

  // If there's an "else" block, handle it now
  if (node->else_stmt() != nullptr) {
    if (!ast::Stmt::IsTerminating(node->then_stmt())) {
      if_builder.JumpToEnd();
    }
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
    VisitExpressionForTest(node->condition(), &loop_body_label, loop_builder.GetBreakLabel(),
                           TestFallthrough::Then);
  }

  VisitIterationStatement(node, &loop_builder);

  if (node->next() != nullptr) {
    Visit(node->next());
  }

  loop_builder.JumpToHeader();
}

void BytecodeGenerator::VisitForInStmt(UNUSED ast::ForInStmt *node) {
  TPL_ASSERT(false, "For-in statements not supported");
}

void BytecodeGenerator::VisitFieldDecl(ast::FieldDecl *node) { AstVisitor::VisitFieldDecl(node); }

void BytecodeGenerator::VisitFunctionDecl(ast::FunctionDecl *node) {
  // The function's TPL type
  auto *func_type = node->type_repr()->type()->As<ast::FunctionType>();

  // Allocate the function
  FunctionInfo *func_info = AllocateFunc(node->name().data(), func_type);

  {
    // Visit the body of the function. We use this handy scope object to track
    // the start and end position of this function's bytecode in the module's
    // bytecode array. Upon destruction, the scoped class will set the bytecode
    // range in the function.
    BytecodePositionScope position_scope(this, func_info);
    Visit(node->function());
  }
}

void BytecodeGenerator::VisitIdentifierExpr(ast::IdentifierExpr *node) {
  // Lookup the local in the current function. It must be there through a
  // previous variable declaration (or parameter declaration). What is returned
  // is a pointer to the variable.

  const std::string local_name = node->name().data();
  LocalVar local = GetCurrentFunction()->LookupLocal(local_name);

  if (GetExecutionResult()->IsLValue()) {
    GetExecutionResult()->SetDestination(local);
    return;
  }

  // The caller wants the R-Value of the identifier. So, we need to load it. If
  // the caller did not provide a destination register, we're done. If the
  // caller provided a destination, we need to move the value of the identifier
  // into the provided destination.

  if (!GetExecutionResult()->HasDestination()) {
    GetExecutionResult()->SetDestination(local.ValueOf());
    return;
  }

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->type());

  // If the local we want the R-Value of is a parameter, we can't take its
  // pointer for the deref, so we use an assignment. Otherwise, a deref is good.
  if (auto *local_info = GetCurrentFunction()->LookupLocalInfoByName(local_name);
      local_info->IsParameter()) {
    BuildAssign(dest, local.ValueOf(), node->type());
  } else {
    BuildDeref(dest, local, node->type());
  }

  GetExecutionResult()->SetDestination(dest);
}

void BytecodeGenerator::VisitImplicitCastExpr(ast::ImplicitCastExpr *node) {
  LocalVar input = VisitExpressionForRValue(node->input());

  switch (node->cast_kind()) {
    case ast::CastKind::SqlBoolToBool: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->type());
      GetEmitter()->Emit(Bytecode::ForceBoolTruth, dest, input);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::CastKind::IntToSqlInt: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->type());
      GetEmitter()->Emit(Bytecode::InitInteger, dest, input);
      GetExecutionResult()->SetDestination(dest);
      break;
    }
    case ast::CastKind::BitCast:
    case ast::CastKind::IntegralCast: {
      // As an optimization, we only issue a new assignment if the input and
      // output types of the cast have different sizes.
      if (node->input()->type()->size() != node->type()->size()) {
        LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->type());
        BuildAssign(dest, input, node->type());
        GetExecutionResult()->SetDestination(dest.ValueOf());
      } else {
        GetExecutionResult()->SetDestination(input);
      }
      break;
    }
    case ast::CastKind::FloatToSqlReal: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->type());
      GetEmitter()->Emit(Bytecode::InitReal, dest, input);
      GetExecutionResult()->SetDestination(dest);
      break;
    }
    case ast::CastKind::SqlIntToSqlReal: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->type());
      GetEmitter()->Emit(Bytecode::IntegerToReal, dest, input);
      GetExecutionResult()->SetDestination(dest);
      break;
    }
    default: { throw std::runtime_error("Implement this cast type"); }
  }
}

void BytecodeGenerator::VisitArrayIndexExpr(ast::IndexExpr *node) {
  // The type and the element's size
  auto *type = node->object()->type()->As<ast::ArrayType>();
  auto elem_size = type->element_type()->size();

  // First, we need to get the base address of the array
  LocalVar arr;
  if (type->HasKnownLength()) {
    arr = VisitExpressionForLValue(node->object());
  } else {
    arr = VisitExpressionForRValue(node->object());
  }

  // The next step is to compute the address of the element at the desired index
  // stored in the IndexExpr node. There are two cases we handle:
  //
  // 1. The index is a constant literal
  // 2. The index is variable
  //
  // If the index is a constant literal (e.g., x[4]), then we can immediately
  // compute the offset of the element, and issue a vanilla Lea instruction.
  //
  // If the index is not a constant, we need to evaluate the expression to
  // produce the index, then issue a LeaScaled instruction to compute the
  // address.

  LocalVar elem_ptr = GetCurrentFunction()->NewLocal(node->type()->PointerTo());

  if (node->index()->IsIntegerLiteral()) {
    const int32_t index = node->index()->As<ast::LitExpr>()->int32_val();
    TPL_ASSERT(index >= 0, "Array indexes must be non-negative");
    GetEmitter()->EmitLea(elem_ptr, arr, (elem_size * index));
  } else {
    LocalVar index = VisitExpressionForRValue(node->index());
    GetEmitter()->EmitLeaScaled(elem_ptr, arr, index, elem_size, 0);
  }

  elem_ptr = elem_ptr.ValueOf();

  if (GetExecutionResult()->IsLValue()) {
    GetExecutionResult()->SetDestination(elem_ptr);
    return;
  }

  // The caller wants the value of the array element. We just computed the
  // element's pointer (in element_ptr). Just dereference it into the desired
  // location and be done with it.

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->type());
  BuildDeref(dest, elem_ptr, node->type());
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitMapIndexExpr(ast::IndexExpr *node) {}

void BytecodeGenerator::VisitIndexExpr(ast::IndexExpr *node) {
  if (node->object()->type()->IsArrayType()) {
    VisitArrayIndexExpr(node);
  } else {
    VisitMapIndexExpr(node);
  }
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
  LocalVar local = GetCurrentFunction()->NewLocal(type, node->name().data());

  // If there's an initializer, generate code for it now
  if (node->initial() != nullptr) {
    VisitExpressionForRValue(node->initial(), local);
  }
}

void BytecodeGenerator::VisitAddressOfExpr(ast::UnaryOpExpr *op) {
  TPL_ASSERT(GetExecutionResult()->IsRValue(), "Address-of expressions must be R-values!");
  LocalVar addr = VisitExpressionForLValue(op->expr());
  if (GetExecutionResult()->HasDestination()) {
    LocalVar dest = GetExecutionResult()->GetDestination();
    BuildAssign(dest, addr, op->type());
  } else {
    GetExecutionResult()->SetDestination(addr);
  }
}

void BytecodeGenerator::VisitDerefExpr(ast::UnaryOpExpr *op) {
  LocalVar addr = VisitExpressionForRValue(op->expr());
  if (GetExecutionResult()->IsLValue()) {
    GetExecutionResult()->SetDestination(addr);
  } else {
    LocalVar dest = GetExecutionResult()->GetOrCreateDestination(op->type());
    BuildDeref(dest, addr, op->type());
    GetExecutionResult()->SetDestination(dest.ValueOf());
  }
}

void BytecodeGenerator::VisitArithmeticUnaryExpr(ast::UnaryOpExpr *op) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(op->type());
  LocalVar input = VisitExpressionForRValue(op->expr());

  Bytecode bytecode;
  switch (op->op()) {
    case parsing::Token::Type::MINUS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Neg), op->type());
      break;
    }
    case parsing::Token::Type::BIT_NOT: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitNeg), op->type());
      break;
    }
    default: { UNREACHABLE("Impossible unary operation"); }
  }

  // Emit
  GetEmitter()->EmitUnaryOp(bytecode, dest, input);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitLogicalNotExpr(ast::UnaryOpExpr *op) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(op->type());
  LocalVar input = VisitExpressionForRValue(op->expr());
  GetEmitter()->EmitUnaryOp(Bytecode::Not, dest, input);
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitUnaryOpExpr(ast::UnaryOpExpr *node) {
  switch (node->op()) {
    case parsing::Token::Type::AMPERSAND: {
      VisitAddressOfExpr(node);
      break;
    }
    case parsing::Token::Type::STAR: {
      VisitDerefExpr(node);
      break;
    }
    case parsing::Token::Type::MINUS:
    case parsing::Token::Type::BIT_NOT: {
      VisitArithmeticUnaryExpr(node);
      break;
    }
    case parsing::Token::Type::BANG: {
      VisitLogicalNotExpr(node);
      break;
    }
    default: { UNREACHABLE("Impossible unary operation"); }
  }
}

void BytecodeGenerator::VisitReturnStmt(ast::ReturnStmt *node) {
  if (node->ret() != nullptr) {
    LocalVar rv = GetCurrentFunction()->GetReturnValueLocal();
    LocalVar result = VisitExpressionForRValue(node->ret());
    BuildAssign(rv.ValueOf(), result, node->ret()->type());
  }
  GetEmitter()->EmitReturn();
}

void BytecodeGenerator::VisitSqlConversionCall(ast::CallExpr *call, ast::Builtin builtin) {
  TPL_ASSERT(call->type() != nullptr, "No return type set for call!");

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->type());

  switch (builtin) {
    case ast::Builtin::BoolToSql: {
      auto input = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::InitBool, dest, input);
      break;
    }
    case ast::Builtin::IntToSql: {
      auto input = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::InitInteger, dest, input);
      break;
    }
    case ast::Builtin::FloatToSql: {
      auto input = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::InitReal, dest, input);
      break;
    }
    case ast::Builtin::DateToSql: {
      auto year = VisitExpressionForRValue(call->arguments()[0]);
      auto month = VisitExpressionForRValue(call->arguments()[1]);
      auto day = VisitExpressionForRValue(call->arguments()[2]);
      GetEmitter()->Emit(Bytecode::InitDate, dest, year, month, day);
      break;
    }
    case ast::Builtin::StringToSql: {
      auto string_lit = call->arguments()[0]->As<ast::LitExpr>()->raw_string_val();
      auto static_string = NewStaticString(call->type()->context(), string_lit);
      GetEmitter()->EmitInitString(dest, static_string, string_lit.length());
      break;
    }
    case ast::Builtin::SqlToBool: {
      auto input = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::ForceBoolTruth, dest, input);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    default: { UNREACHABLE("Impossible SQL conversion call"); }
  }
}

void BytecodeGenerator::VisitSqlStringLikeCall(ast::CallExpr *call) {
  auto dest = GetExecutionResult()->GetOrCreateDestination(call->type());
  auto input = VisitExpressionForLValue(call->arguments()[0]);
  auto pattern = VisitExpressionForLValue(call->arguments()[1]);
  GetEmitter()->Emit(Bytecode::Like, dest, input, pattern);
  GetExecutionResult()->SetDestination(dest);
}

void BytecodeGenerator::VisitBuiltinTableIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  // The first argument to all calls is a pointer to the TVI
  LocalVar iter = VisitExpressionForRValue(call->arguments()[0]);

  switch (builtin) {
    case ast::Builtin::TableIterInit: {
      // The second argument is the table name as a literal string
      TPL_ASSERT(call->arguments()[1]->IsStringLiteral(), "Table name must be a string literal");
      ast::Identifier table_name = call->arguments()[1]->As<ast::LitExpr>()->raw_string_val();
      sql::Table *table = sql::Catalog::Instance()->LookupTableByName(table_name);
      TPL_ASSERT(table != nullptr, "Table does not exist!");
      GetEmitter()->EmitTableIterInit(Bytecode::TableVectorIteratorInit, iter, table->GetId());
      GetEmitter()->Emit(Bytecode::TableVectorIteratorPerformInit, iter);
      break;
    }
    case ast::Builtin::TableIterAdvance: {
      LocalVar cond = GetExecutionResult()->GetOrCreateDestination(call->type());
      GetEmitter()->Emit(Bytecode::TableVectorIteratorNext, cond, iter);
      GetExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::TableIterGetVPI: {
      LocalVar vpi = GetExecutionResult()->GetOrCreateDestination(call->type());
      GetEmitter()->Emit(Bytecode::TableVectorIteratorGetVPI, vpi, iter);
      GetExecutionResult()->SetDestination(vpi.ValueOf());
      break;
    }
    case ast::Builtin::TableIterClose: {
      GetEmitter()->Emit(Bytecode::TableVectorIteratorFree, iter);
      break;
    }
    default: { UNREACHABLE("Impossible table iteration call"); }
  }
}

void BytecodeGenerator::VisitBuiltinTableIterParallelCall(ast::CallExpr *call) {
  // First is the table name as a string literal
  const auto table_name = call->arguments()[0]->As<ast::LitExpr>()->raw_string_val();
  sql::Table *table = sql::Catalog::Instance()->LookupTableByName(table_name);
  TPL_ASSERT(table != nullptr, "Table does not exist!");

  // Next is the execution context
  LocalVar exec_ctx = VisitExpressionForRValue(call->arguments()[1]);

  // Next is the thread state container
  LocalVar thread_state_container = VisitExpressionForRValue(call->arguments()[2]);

  // Finally the scan function as an identifier
  const auto scan_fn_name = call->arguments()[3]->As<ast::IdentifierExpr>()->name();

  // Done
  GetEmitter()->EmitParallelTableScan(table->GetId(), exec_ctx, thread_state_container,
                                      LookupFuncIdByName(scan_fn_name.data()));
}

void BytecodeGenerator::VisitBuiltinVPICall(ast::CallExpr *call, ast::Builtin builtin) {
  TPL_ASSERT(call->type() != nullptr, "No return type set for call!");

  // The first argument to all calls is a pointer to the VPI
  LocalVar vpi = VisitExpressionForRValue(call->arguments()[0]);

  switch (builtin) {
    case ast::Builtin::VPIInit: {
      LocalVar vector_projection = VisitExpressionForRValue(call->arguments()[1]);
      if (call->arguments().size() == 3) {
        LocalVar tid_list = VisitExpressionForRValue(call->arguments()[2]);
        GetEmitter()->Emit(Bytecode::VPIInitWithList, vpi, vector_projection, tid_list);
      } else {
        GetEmitter()->Emit(Bytecode::VPIInit, vpi, vector_projection);
      }
      break;
    }
    case ast::Builtin::VPIFree: {
      GetEmitter()->Emit(Bytecode::VPIFree, vpi);
      break;
    }
    case ast::Builtin::VPIIsFiltered: {
      LocalVar is_filtered = GetExecutionResult()->GetOrCreateDestination(call->type());
      GetEmitter()->Emit(Bytecode::VPIIsFiltered, is_filtered, vpi);
      GetExecutionResult()->SetDestination(is_filtered.ValueOf());
      break;
    }
    case ast::Builtin::VPIGetSelectedRowCount: {
      LocalVar count = GetExecutionResult()->GetOrCreateDestination(call->type());
      GetEmitter()->Emit(Bytecode::VPIGetSelectedRowCount, count, vpi);
      GetExecutionResult()->SetDestination(count.ValueOf());
      break;
    }
    case ast::Builtin::VPIGetVectorProjection: {
      LocalVar vector_projection = GetExecutionResult()->GetOrCreateDestination(call->type());
      GetEmitter()->Emit(Bytecode::VPIGetVectorProjection, vector_projection, vpi);
      GetExecutionResult()->SetDestination(vector_projection.ValueOf());
      break;
    }
    case ast::Builtin::VPIHasNext:
    case ast::Builtin::VPIHasNextFiltered: {
      const Bytecode bytecode =
          builtin == ast::Builtin::VPIHasNext ? Bytecode::VPIHasNext : Bytecode::VPIHasNextFiltered;
      LocalVar cond = GetExecutionResult()->GetOrCreateDestination(call->type());
      GetEmitter()->Emit(bytecode, cond, vpi);
      GetExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::VPIAdvance: {
      GetEmitter()->Emit(Bytecode::VPIAdvance, vpi);
      break;
    }
    case ast::Builtin::VPIAdvanceFiltered: {
      GetEmitter()->Emit(Bytecode::VPIAdvanceFiltered, vpi);
      break;
    }
    case ast::Builtin::VPISetPosition: {
      LocalVar index = VisitExpressionForRValue(call->arguments()[1]);
      GetEmitter()->Emit(Bytecode::VPISetPosition, vpi, index);
      break;
    }
    case ast::Builtin::VPISetPositionFiltered: {
      LocalVar index = VisitExpressionForRValue(call->arguments()[1]);
      GetEmitter()->Emit(Bytecode::VPISetPositionFiltered, vpi, index);
      break;
    }
    case ast::Builtin::VPIMatch: {
      LocalVar match = VisitExpressionForRValue(call->arguments()[1]);
      GetEmitter()->Emit(Bytecode::VPIMatch, vpi, match);
      break;
    }
    case ast::Builtin::VPIReset: {
      GetEmitter()->Emit(Bytecode::VPIReset, vpi);
      break;
    }
    case ast::Builtin::VPIResetFiltered: {
      GetEmitter()->Emit(Bytecode::VPIResetFiltered, vpi);
      break;
    }
    case ast::Builtin::VPIGetSmallInt:
    case ast::Builtin::VPIGetInt:
    case ast::Builtin::VPIGetBigInt:
    case ast::Builtin::VPIGetReal:
    case ast::Builtin::VPIGetDouble:
    case ast::Builtin::VPIGetDate:
    case ast::Builtin::VPIGetString: {
      Bytecode bytecode;
      if (builtin == ast::Builtin::VPIGetSmallInt) {
        bytecode = Bytecode::VPIGetSmallInt;
      } else if (builtin == ast::Builtin::VPIGetInt) {
        bytecode = Bytecode::VPIGetInteger;
      } else if (builtin == ast::Builtin::VPIGetBigInt) {
        bytecode = Bytecode::VPIGetBigInt;
      } else if (builtin == ast::Builtin::VPIGetReal) {
        bytecode = Bytecode::VPIGetReal;
      } else if (builtin == ast::Builtin::VPIGetDouble) {
        bytecode = Bytecode::VPIGetDouble;
      } else if (builtin == ast::Builtin::VPIGetDate) {
        bytecode = Bytecode::VPIGetDate;
      } else {
        bytecode = Bytecode::VPIGetString;
      }
      LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->type());
      const uint32_t col_idx = call->arguments()[1]->As<ast::LitExpr>()->int32_val();
      GetEmitter()->EmitVPIGet(bytecode, result, vpi, col_idx);
      break;
    }
    case ast::Builtin::VPISetSmallInt:
    case ast::Builtin::VPISetInt:
    case ast::Builtin::VPISetBigInt:
    case ast::Builtin::VPISetReal:
    case ast::Builtin::VPISetDouble:
    case ast::Builtin::VPISetDate:
    case ast::Builtin::VPISetString: {
      Bytecode bytecode;
      if (builtin == ast::Builtin::VPISetSmallInt) {
        bytecode = Bytecode::VPISetSmallInt;
      } else if (builtin == ast::Builtin::VPISetInt) {
        bytecode = Bytecode::VPISetInteger;
      } else if (builtin == ast::Builtin::VPISetBigInt) {
        bytecode = Bytecode::VPISetBigInt;
      } else if (builtin == ast::Builtin::VPISetReal) {
        bytecode = Bytecode::VPISetReal;
      } else if (builtin == ast::Builtin::VPISetDouble) {
        bytecode = Bytecode::VPISetDouble;
      } else if (builtin == ast::Builtin::VPISetDate) {
        bytecode = Bytecode::VPISetDate;
      } else {
        bytecode = Bytecode::VPISetString;
      }
      auto input = VisitExpressionForLValue(call->arguments()[1]);
      auto col_idx = call->arguments()[2]->As<ast::LitExpr>()->int32_val();
      GetEmitter()->EmitVPISet(bytecode, vpi, input, col_idx);
      break;
    }
    default: { UNREACHABLE("Impossible table iteration call"); }
  }
}

void BytecodeGenerator::VisitBuiltinHashCall(ast::CallExpr *call) {
  TPL_ASSERT(call->type()->IsSpecificBuiltin(ast::BuiltinType::Uint64),
             "Return type of @hash(...) expected to be 8-byte unsigned hash");
  TPL_ASSERT(!call->arguments().empty(), "@hash() must contain at least one input argument");
  TPL_ASSERT(GetExecutionResult() != nullptr, "Caller of @hash() must use result");

  // The running hash value initialized to zero
  LocalVar hash_val = GetExecutionResult()->GetOrCreateDestination(call->type());

  GetEmitter()->EmitAssignImm8(hash_val, 0);

  for (uint32_t idx = 0; idx < call->num_args(); idx++) {
    TPL_ASSERT(call->arguments()[idx]->type()->IsSqlValueType(),
               "Input to hash must be a SQL value type");

    LocalVar input = VisitExpressionForLValue(call->arguments()[idx]);
    const auto *type = call->arguments()[idx]->type()->As<ast::BuiltinType>();
    switch (type->kind()) {
      case ast::BuiltinType::Integer: {
        GetEmitter()->Emit(Bytecode::HashInt, hash_val, input, hash_val.ValueOf());
        break;
      }
      case ast::BuiltinType::Real: {
        GetEmitter()->Emit(Bytecode::HashReal, hash_val, input, hash_val.ValueOf());
        break;
      }
      case ast::BuiltinType::StringVal: {
        GetEmitter()->Emit(Bytecode::HashString, hash_val, input, hash_val.ValueOf());
        break;
      }
      case ast::BuiltinType::Date: {
        GetEmitter()->Emit(Bytecode::HashDate, hash_val, input, hash_val.ValueOf());
        break;
      }
      default: { UNREACHABLE("Hashing this type isn't supported!"); }
    }
  }

  // Set return
  GetExecutionResult()->SetDestination(hash_val.ValueOf());
}

void BytecodeGenerator::VisitBuiltinFilterManagerCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar filter_manager = VisitExpressionForRValue(call->arguments()[0]);
  switch (builtin) {
    case ast::Builtin::FilterManagerInit: {
      GetEmitter()->Emit(Bytecode::FilterManagerInit, filter_manager);
      break;
    }
    case ast::Builtin::FilterManagerInsertFilter: {
      GetEmitter()->Emit(Bytecode::FilterManagerStartNewClause, filter_manager);

      // Insert all flavors
      for (uint32_t arg_idx = 1; arg_idx < call->num_args(); arg_idx++) {
        const std::string func_name =
            call->arguments()[arg_idx]->As<ast::IdentifierExpr>()->name().data();
        const FunctionId func_id = LookupFuncIdByName(func_name);
        GetEmitter()->EmitFilterManagerInsertFilter(filter_manager, func_id);
      }
      break;
    }
    case ast::Builtin::FilterManagerFinalize: {
      GetEmitter()->Emit(Bytecode::FilterManagerFinalize, filter_manager);
      break;
    }
    case ast::Builtin::FilterManagerRunFilters: {
      LocalVar vpi = VisitExpressionForRValue(call->arguments()[1]);
      GetEmitter()->Emit(Bytecode::FilterManagerRunFilters, filter_manager, vpi);
      break;
    }
    case ast::Builtin::FilterManagerFree: {
      GetEmitter()->Emit(Bytecode::FilterManagerFree, filter_manager);
      break;
    }
    default: { UNREACHABLE("Impossible filter manager call"); }
  }
}

void BytecodeGenerator::VisitBuiltinVectorFilterCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar vector_projection = VisitExpressionForRValue(call->arguments()[0]);
  LocalVar tid_list = VisitExpressionForRValue(call->arguments()[3]);

#define GEN_CASE(BYTECODE)                                                               \
  LocalVar left_col = VisitExpressionForRValue(call->arguments()[1]);                    \
  if (!call->arguments()[2]->type()->IsIntegerType()) {                                  \
    LocalVar right_val = VisitExpressionForLValue(call->arguments()[2]);                 \
    GetEmitter()->Emit(BYTECODE##Val, vector_projection, left_col, right_val, tid_list); \
  } else {                                                                               \
    LocalVar right_col = VisitExpressionForRValue(call->arguments()[2]);                 \
    GetEmitter()->Emit(BYTECODE, vector_projection, left_col, right_col, tid_list);      \
  }

  switch (builtin) {
    case ast::Builtin::VectorFilterEqual: {
      GEN_CASE(Bytecode::VectorFilterEqual);
      break;
    }
    case ast::Builtin::VectorFilterGreaterThan: {
      GEN_CASE(Bytecode::VectorFilterGreaterThan);
      break;
    }
    case ast::Builtin::VectorFilterGreaterThanEqual: {
      GEN_CASE(Bytecode::VectorFilterGreaterThanEqual);
      break;
    }
    case ast::Builtin::VectorFilterLessThan: {
      GEN_CASE(Bytecode::VectorFilterLessThan);
      break;
    }
    case ast::Builtin::VectorFilterLessThanEqual: {
      GEN_CASE(Bytecode::VectorFilterLessThanEqual);
      break;
    }
    case ast::Builtin::VectorFilterNotEqual: {
      GEN_CASE(Bytecode::VectorFilterNotEqual);
      break;
    }
    default: { UNREACHABLE("Impossible vector filter executor call"); }
  }
#undef GEN_CASE
}

void BytecodeGenerator::VisitBuiltinAggHashTableCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggHashTableInit: {
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar memory = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar entry_size = VisitExpressionForRValue(call->arguments()[2]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableInit, agg_ht, memory, entry_size);
      break;
    }
    case ast::Builtin::AggHashTableInsert: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->type());
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar hash = VisitExpressionForRValue(call->arguments()[1]);
      Bytecode bytecode = Bytecode::AggregationHashTableAllocTuple;
      if (call->arguments().size() > 2) {
        TPL_ASSERT(call->arguments()[2]->IsBoolLiteral(),
                   "Last argument must be a boolean literal");
        const bool partitioned = call->arguments()[2]->As<ast::LitExpr>()->bool_val();
        bytecode = partitioned ? Bytecode::AggregationHashTableAllocTuplePartitioned
                               : Bytecode::AggregationHashTableAllocTuple;
      }
      GetEmitter()->Emit(bytecode, dest, agg_ht, hash);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableLinkEntry: {
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar entry = VisitExpressionForRValue(call->arguments()[1]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableLinkHashTableEntry, agg_ht, entry);
      break;
    }
    case ast::Builtin::AggHashTableLookup: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->type());
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar hash = VisitExpressionForRValue(call->arguments()[1]);
      auto key_eq_fn =
          LookupFuncIdByName(call->arguments()[2]->As<ast::IdentifierExpr>()->name().data());
      LocalVar arg = VisitExpressionForRValue(call->arguments()[3]);
      GetEmitter()->EmitAggHashTableLookup(dest, agg_ht, hash, key_eq_fn, arg);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableProcessBatch: {
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar iters = VisitExpressionForRValue(call->arguments()[1]);
      auto hash_fn =
          LookupFuncIdByName(call->arguments()[2]->As<ast::IdentifierExpr>()->name().data());
      auto key_eq_fn =
          LookupFuncIdByName(call->arguments()[3]->As<ast::IdentifierExpr>()->name().data());
      auto init_agg_fn =
          LookupFuncIdByName(call->arguments()[4]->As<ast::IdentifierExpr>()->name().data());
      auto merge_agg_fn =
          LookupFuncIdByName(call->arguments()[5]->As<ast::IdentifierExpr>()->name().data());
      LocalVar partitioned = VisitExpressionForRValue(call->arguments()[6]);
      GetEmitter()->EmitAggHashTableProcessBatch(agg_ht, iters, hash_fn, key_eq_fn, init_agg_fn,
                                                 merge_agg_fn, partitioned);
      break;
    }
    case ast::Builtin::AggHashTableMovePartitions: {
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar aht_offset = VisitExpressionForRValue(call->arguments()[2]);
      auto merge_part_fn =
          LookupFuncIdByName(call->arguments()[3]->As<ast::IdentifierExpr>()->name().data());
      GetEmitter()->EmitAggHashTableMovePartitions(agg_ht, tls, aht_offset, merge_part_fn);
      break;
    }
    case ast::Builtin::AggHashTableParallelPartitionedScan: {
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar ctx = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar tls = VisitExpressionForRValue(call->arguments()[2]);
      auto scan_part_fn =
          LookupFuncIdByName(call->arguments()[3]->As<ast::IdentifierExpr>()->name().data());
      GetEmitter()->EmitAggHashTableParallelPartitionedScan(agg_ht, ctx, tls, scan_part_fn);
      break;
    }
    case ast::Builtin::AggHashTableFree: {
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableFree, agg_ht);
      break;
    }
    default: { UNREACHABLE("Impossible aggregation hash table bytecode"); }
  }
}

void BytecodeGenerator::VisitBuiltinAggHashTableIterCall(ast::CallExpr *call,
                                                         ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggHashTableIterInit: {
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[1]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableIteratorInit, agg_ht_iter, agg_ht);
      break;
    }
    case ast::Builtin::AggHashTableIterHasNext: {
      LocalVar has_more = GetExecutionResult()->GetOrCreateDestination(call->type());
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableIteratorHasNext, has_more, agg_ht_iter);
      GetExecutionResult()->SetDestination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableIterNext: {
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableIteratorNext, agg_ht_iter);
      break;
    }
    case ast::Builtin::AggHashTableIterGetRow: {
      LocalVar row_ptr = GetExecutionResult()->GetOrCreateDestination(call->type());
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableIteratorGetRow, row_ptr, agg_ht_iter);
      GetExecutionResult()->SetDestination(row_ptr.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableIterClose: {
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableIteratorFree, agg_ht_iter);
      break;
    }
    default: { UNREACHABLE("Impossible aggregation hash table iteration bytecode"); }
  }
}

void BytecodeGenerator::VisitBuiltinAggPartIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggPartIterHasNext: {
      LocalVar has_more = GetExecutionResult()->GetOrCreateDestination(call->type());
      LocalVar iter = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorHasNext, has_more, iter);
      GetExecutionResult()->SetDestination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::AggPartIterNext: {
      LocalVar iter = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorNext, iter);
      break;
    }
    case ast::Builtin::AggPartIterGetRow: {
      LocalVar row = GetExecutionResult()->GetOrCreateDestination(call->type());
      LocalVar iter = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorGetRow, row, iter);
      GetExecutionResult()->SetDestination(row.ValueOf());
      break;
    }
    case ast::Builtin::AggPartIterGetRowEntry: {
      LocalVar entry = GetExecutionResult()->GetOrCreateDestination(call->type());
      LocalVar iter = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorGetRowEntry, entry, iter);
      GetExecutionResult()->SetDestination(entry.ValueOf());
      break;
    }
    case ast::Builtin::AggPartIterGetHash: {
      LocalVar hash = GetExecutionResult()->GetOrCreateDestination(call->type());
      LocalVar iter = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorGetHash, hash, iter);
      GetExecutionResult()->SetDestination(hash.ValueOf());
      break;
    }
    default: { UNREACHABLE("Impossible aggregation partition iterator bytecode"); }
  }
}

namespace {

// clang-format off
#define AGG_CODES(F) \
  F(CountAggregate,      CountAggregateInit,      CountAggregateAdvance,      CountAggregateGetResult,      CountAggregateMerge,      CountAggregateReset)                \
  F(CountStarAggregate,  CountStarAggregateInit,  CountStarAggregateAdvance,  CountStarAggregateGetResult,  CountStarAggregateMerge,  CountStarAggregateReset)            \
  F(AvgAggregate,        AvgAggregateInit,        AvgAggregateAdvanceInteger, AvgAggregateGetResult,        AvgAggregateMerge,        AvgAggregateReset)                  \
  F(IntegerMaxAggregate, IntegerMaxAggregateInit, IntegerMaxAggregateAdvance, IntegerMaxAggregateGetResult, IntegerMaxAggregateMerge, IntegerMaxAggregateReset)           \
  F(IntegerMinAggregate, IntegerMinAggregateInit, IntegerMinAggregateAdvance, IntegerMinAggregateGetResult, IntegerMinAggregateMerge, IntegerMinAggregateReset)           \
  F(IntegerSumAggregate, IntegerSumAggregateInit, IntegerSumAggregateAdvance, IntegerSumAggregateGetResult, IntegerSumAggregateMerge, IntegerSumAggregateReset)           \
  F(RealMaxAggregate,    RealMaxAggregateInit,    RealMaxAggregateAdvance,    RealMaxAggregateGetResult,    RealMaxAggregateMerge,    RealMaxAggregateReset)              \
  F(RealMinAggregate,    RealMinAggregateInit,    RealMinAggregateAdvance,    RealMinAggregateGetResult,    RealMinAggregateMerge,    RealMinAggregateReset)              \
  F(RealSumAggregate,    RealSumAggregateInit,    RealSumAggregateAdvance,    RealSumAggregateGetResult,    RealSumAggregateMerge,    RealSumAggregateReset)

// clang-format on

enum class AggOpKind : uint8_t { Init = 0, Advance = 1, GetResult = 2, Merge = 3, Reset = 4 };

// Given an aggregate kind and the operation to perform on it, determine the
// appropriate bytecode
template <AggOpKind OpKind>
Bytecode OpForAgg(ast::BuiltinType::Kind agg_kind);

template <>
Bytecode OpForAgg<AggOpKind::Init>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: { UNREACHABLE("Impossible aggregate type"); }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset) \
  case ast::BuiltinType::Type:                              \
    return Bytecode::Init;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

template <>
Bytecode OpForAgg<AggOpKind::Advance>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: { UNREACHABLE("Impossible aggregate type"); }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset) \
  case ast::BuiltinType::Type:                              \
    return Bytecode::Advance;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

template <>
Bytecode OpForAgg<AggOpKind::GetResult>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: { UNREACHABLE("Impossible aggregate type"); }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset) \
  case ast::BuiltinType::Type:                              \
    return Bytecode::GetResult;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

template <>
Bytecode OpForAgg<AggOpKind::Merge>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: { UNREACHABLE("Impossible aggregate type"); }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset) \
  case ast::BuiltinType::Type:                              \
    return Bytecode::Merge;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

template <>
Bytecode OpForAgg<AggOpKind::Reset>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: { UNREACHABLE("Impossible aggregate type"); }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset) \
  case ast::BuiltinType::Type:                              \
    return Bytecode::Reset;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

}  // namespace

void BytecodeGenerator::VisitBuiltinAggregatorCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggInit:
    case ast::Builtin::AggReset: {
      for (const auto &arg : call->arguments()) {
        const auto agg_kind = arg->type()->GetPointeeType()->As<ast::BuiltinType>()->kind();
        LocalVar input = VisitExpressionForRValue(arg);
        Bytecode bytecode;
        if (builtin == ast::Builtin::AggInit) {
          bytecode = OpForAgg<AggOpKind::Init>(agg_kind);
        } else {
          bytecode = OpForAgg<AggOpKind::Reset>(agg_kind);
        }
        GetEmitter()->Emit(bytecode, input);
      }
      break;
    }
    case ast::Builtin::AggAdvance: {
      const auto &args = call->arguments();
      const auto agg_kind = args[0]->type()->GetPointeeType()->As<ast::BuiltinType>()->kind();
      LocalVar agg = VisitExpressionForRValue(args[0]);
      LocalVar input = VisitExpressionForRValue(args[1]);
      Bytecode bytecode = OpForAgg<AggOpKind::Advance>(agg_kind);

      // Hack to handle advancing AvgAggregates with float/double precision numbers. The default
      // behavior in OpForAgg() is to use AvgAggregateAdvanceInteger.
      if (agg_kind == ast::BuiltinType::AvgAggregate &&
          args[1]->type()->GetPointeeType()->IsSpecificBuiltin(ast::BuiltinType::Real)) {
        bytecode = Bytecode::AvgAggregateAdvanceReal;
      }

      GetEmitter()->Emit(bytecode, agg, input);
      break;
    }
    case ast::Builtin::AggMerge: {
      const auto &args = call->arguments();
      const auto agg_kind = args[0]->type()->GetPointeeType()->As<ast::BuiltinType>()->kind();
      LocalVar agg_1 = VisitExpressionForRValue(args[0]);
      LocalVar agg_2 = VisitExpressionForRValue(args[1]);
      Bytecode bytecode = OpForAgg<AggOpKind::Merge>(agg_kind);
      GetEmitter()->Emit(bytecode, agg_1, agg_2);
      break;
    }
    case ast::Builtin::AggResult: {
      const auto &args = call->arguments();
      const auto agg_kind = args[0]->type()->GetPointeeType()->As<ast::BuiltinType>()->kind();
      LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->type());
      LocalVar agg = VisitExpressionForRValue(args[0]);
      Bytecode bytecode = OpForAgg<AggOpKind::GetResult>(agg_kind);
      GetEmitter()->Emit(bytecode, result, agg);
      break;
    }
    default: { UNREACHABLE("Impossible aggregator call"); }
  }
}

void BytecodeGenerator::VisitBuiltinJoinHashTableCall(ast::CallExpr *call, ast::Builtin builtin) {
  // The join hash table is always the first argument to all JHT calls
  LocalVar join_hash_table = VisitExpressionForRValue(call->arguments()[0]);

  switch (builtin) {
    case ast::Builtin::JoinHashTableInit: {
      LocalVar memory = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar entry_size = VisitExpressionForRValue(call->arguments()[2]);
      GetEmitter()->Emit(Bytecode::JoinHashTableInit, join_hash_table, memory, entry_size);
      break;
    }
    case ast::Builtin::JoinHashTableInsert: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->type());
      LocalVar hash = VisitExpressionForRValue(call->arguments()[1]);
      GetEmitter()->Emit(Bytecode::JoinHashTableAllocTuple, dest, join_hash_table, hash);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::JoinHashTableBuild: {
      GetEmitter()->Emit(Bytecode::JoinHashTableBuild, join_hash_table);
      break;
    }
    case ast::Builtin::JoinHashTableBuildParallel: {
      LocalVar tls = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar jht_offset = VisitExpressionForRValue(call->arguments()[2]);
      GetEmitter()->Emit(Bytecode::JoinHashTableBuildParallel, join_hash_table, tls, jht_offset);
      break;
    }
    case ast::Builtin::JoinHashTableLookup: {
      LocalVar ht_entry_iter = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar hash = VisitExpressionForRValue(call->arguments()[2]);
      GetEmitter()->Emit(Bytecode::JoinHashTableLookup, join_hash_table, ht_entry_iter, hash);
      break;
    }
    case ast::Builtin::JoinHashTableFree: {
      GetEmitter()->Emit(Bytecode::JoinHashTableFree, join_hash_table);
      break;
    }
    default: { UNREACHABLE("Impossible join hash table call"); }
  }
}

void BytecodeGenerator::VisitBuiltinHashTableEntryIteratorCall(ast::CallExpr *call,
                                                               ast::Builtin builtin) {
  // The hash table entry iterator is always the first argument to all calls
  LocalVar ht_entry_iter = VisitExpressionForRValue(call->arguments()[0]);

  switch (builtin) {
    case ast::Builtin::HashTableEntryIterHasNext: {
      LocalVar has_more = GetExecutionResult()->GetOrCreateDestination(call->type());
      const std::string key_eq_func_name =
          call->arguments()[1]->As<ast::IdentifierExpr>()->name().data();
      LocalVar ctx = VisitExpressionForRValue(call->arguments()[2]);
      LocalVar probe_tuple = VisitExpressionForRValue(call->arguments()[3]);
      GetEmitter()->EmitHashTableEntryIteratorHasNext(
          has_more, ht_entry_iter, LookupFuncIdByName(key_eq_func_name), ctx, probe_tuple);
      GetExecutionResult()->SetDestination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::HashTableEntryIterGetRow: {
      LocalVar row = GetExecutionResult()->GetOrCreateDestination(call->type());
      GetEmitter()->Emit(Bytecode::HashTableEntryIteratorGetRow, row, ht_entry_iter);
      break;
    }
    default: { UNREACHABLE("Impossible hash table entry iterator call"); }
  }
}

void BytecodeGenerator::VisitBuiltinSorterCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::SorterInit: {
      // TODO(pmenon): Fix me so that the comparison function doesn't have be
      // listed by name.
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar memory = VisitExpressionForRValue(call->arguments()[1]);
      const std::string cmp_func_name =
          call->arguments()[2]->As<ast::IdentifierExpr>()->name().data();
      LocalVar entry_size = VisitExpressionForRValue(call->arguments()[3]);
      GetEmitter()->EmitSorterInit(Bytecode::SorterInit, sorter, memory,
                                   LookupFuncIdByName(cmp_func_name), entry_size);
      break;
    }
    case ast::Builtin::SorterInsert: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->type());
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::SorterAllocTuple, dest, sorter);
      break;
    }
    case ast::Builtin::SorterInsertTopK: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->type());
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar top_k = VisitExpressionForRValue(call->arguments()[1]);
      GetEmitter()->Emit(Bytecode::SorterAllocTupleTopK, dest, sorter, top_k);
      break;
    }
    case ast::Builtin::SorterInsertTopKFinish: {
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar top_k = VisitExpressionForRValue(call->arguments()[1]);
      GetEmitter()->Emit(Bytecode::SorterAllocTupleTopKFinish, sorter, top_k);
      break;
    }
    case ast::Builtin::SorterSort: {
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::SorterSort, sorter);
      break;
    }
    case ast::Builtin::SorterSortParallel: {
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar sorter_offset = VisitExpressionForRValue(call->arguments()[2]);
      GetEmitter()->Emit(Bytecode::SorterSortParallel, sorter, tls, sorter_offset);
      break;
    }
    case ast::Builtin::SorterSortTopKParallel: {
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar sorter_offset = VisitExpressionForRValue(call->arguments()[2]);
      LocalVar top_k = VisitExpressionForRValue(call->arguments()[3]);
      GetEmitter()->Emit(Bytecode::SorterSortTopKParallel, sorter, tls, sorter_offset, top_k);
      break;
    }
    case ast::Builtin::SorterFree: {
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      GetEmitter()->Emit(Bytecode::SorterFree, sorter);
      break;
    }
    default: { UNREACHABLE("Impossible bytecode"); }
  }
}

void BytecodeGenerator::VisitBuiltinSorterIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  // The first argument to all calls is the sorter iterator instance
  const LocalVar sorter_iter = VisitExpressionForRValue(call->arguments()[0]);

  switch (builtin) {
    case ast::Builtin::SorterIterInit: {
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[1]);
      GetEmitter()->Emit(Bytecode::SorterIteratorInit, sorter_iter, sorter);
      break;
    }
    case ast::Builtin::SorterIterHasNext: {
      LocalVar cond = GetExecutionResult()->GetOrCreateDestination(call->type());
      GetEmitter()->Emit(Bytecode::SorterIteratorHasNext, cond, sorter_iter);
      GetExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::SorterIterNext: {
      GetEmitter()->Emit(Bytecode::SorterIteratorNext, sorter_iter);
      break;
    }
    case ast::Builtin::SorterIterGetRow: {
      LocalVar row_ptr = GetExecutionResult()->GetOrCreateDestination(call->type());
      GetEmitter()->Emit(Bytecode::SorterIteratorGetRow, row_ptr, sorter_iter);
      GetExecutionResult()->SetDestination(row_ptr.ValueOf());
      break;
    }
    case ast::Builtin::SorterIterClose: {
      GetEmitter()->Emit(Bytecode::SorterIteratorFree, sorter_iter);
      break;
    }
    default: { UNREACHABLE("Impossible table iteration call"); }
  }
}

void BytecodeGenerator::VisitResultBufferCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar exec_ctx = VisitExpressionForRValue(call->arguments()[0]);
  switch (builtin) {
    case ast::Builtin::ResultBufferAllocOutRow: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->type());
      GetEmitter()->Emit(Bytecode::ResultBufferAllocOutputRow, dest, exec_ctx);
      break;
    }
    case ast::Builtin::ResultBufferFinalize: {
      GetEmitter()->Emit(Bytecode::ResultBufferFinalize, exec_ctx);
      break;
    }
    default: { UNREACHABLE("Invalid result buffer call!"); }
  }
}

void BytecodeGenerator::VisitExecutionContextCall(ast::CallExpr *call,
                                                  UNUSED ast::Builtin builtin) {
  ast::Context *ctx = call->type()->context();

  // The memory pool pointer
  LocalVar mem_pool = GetExecutionResult()->GetOrCreateDestination(
      ast::BuiltinType::Get(ctx, ast::BuiltinType::MemoryPool)->PointerTo());

  // The execution context pointer
  LocalVar exec_ctx = VisitExpressionForRValue(call->arguments()[0]);

  // Emit bytecode
  GetEmitter()->Emit(Bytecode::ExecutionContextGetMemoryPool, mem_pool, exec_ctx);

  // Indicate where the result is
  GetExecutionResult()->SetDestination(mem_pool.ValueOf());
}

void BytecodeGenerator::VisitBuiltinThreadStateContainerCall(ast::CallExpr *call,
                                                             ast::Builtin builtin) {
  LocalVar tls = VisitExpressionForRValue(call->arguments()[0]);
  switch (builtin) {
    case ast::Builtin::ThreadStateContainerInit: {
      LocalVar memory = VisitExpressionForRValue(call->arguments()[1]);
      GetEmitter()->Emit(Bytecode::ThreadStateContainerInit, tls, memory);
      break;
    }
    case ast::Builtin::ThreadStateContainerIterate: {
      LocalVar ctx = VisitExpressionForRValue(call->arguments()[1]);
      FunctionId iterate_fn =
          LookupFuncIdByName(call->arguments()[2]->As<ast::IdentifierExpr>()->name().data());
      GetEmitter()->EmitThreadStateContainerIterate(tls, ctx, iterate_fn);
      break;
    }
    case ast::Builtin::ThreadStateContainerReset: {
      LocalVar entry_size = VisitExpressionForRValue(call->arguments()[1]);
      FunctionId init_fn =
          LookupFuncIdByName(call->arguments()[2]->As<ast::IdentifierExpr>()->name().data());
      FunctionId destroy_fn =
          LookupFuncIdByName(call->arguments()[3]->As<ast::IdentifierExpr>()->name().data());
      LocalVar ctx = VisitExpressionForRValue(call->arguments()[4]);
      GetEmitter()->EmitThreadStateContainerReset(tls, entry_size, init_fn, destroy_fn, ctx);
      break;
    }
    case ast::Builtin::ThreadStateContainerFree: {
      GetEmitter()->Emit(Bytecode::ThreadStateContainerFree, tls);
      break;
    }
    default: { UNREACHABLE("Impossible thread state container call"); }
  }
}

void BytecodeGenerator::VisitBuiltinTrigCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->type());
  LocalVar src = VisitExpressionForRValue(call->arguments()[0]);

  switch (builtin) {
    case ast::Builtin::ACos: {
      GetEmitter()->Emit(Bytecode::Acos, dest, src);
      break;
    }
    case ast::Builtin::ASin: {
      GetEmitter()->Emit(Bytecode::Asin, dest, src);
      break;
    }
    case ast::Builtin::ATan: {
      GetEmitter()->Emit(Bytecode::Atan, dest, src);
      break;
    }
    case ast::Builtin::ATan2: {
      LocalVar src2 = VisitExpressionForRValue(call->arguments()[1]);
      GetEmitter()->Emit(Bytecode::Atan2, dest, src, src2);
      break;
    }
    case ast::Builtin::Cos: {
      GetEmitter()->Emit(Bytecode::Cos, dest, src);
      break;
    }
    case ast::Builtin::Cot: {
      GetEmitter()->Emit(Bytecode::Cot, dest, src);
      break;
    }
    case ast::Builtin::Sin: {
      GetEmitter()->Emit(Bytecode::Sin, dest, src);
      break;
    }
    case ast::Builtin::Tan: {
      GetEmitter()->Emit(Bytecode::Tan, dest, src);
    }
    default: { UNREACHABLE("Impossible trigonometric bytecode"); }
  }

  GetExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitBuiltinSizeOfCall(ast::CallExpr *call) {
  ast::Type *target_type = call->arguments()[0]->type();
  LocalVar size_var = GetExecutionResult()->GetOrCreateDestination(call->type());
  GetEmitter()->EmitAssignImm4(size_var, target_type->size());
  GetExecutionResult()->SetDestination(size_var.ValueOf());
}

void BytecodeGenerator::VisitBuiltinOffsetOfCall(ast::CallExpr *call) {
  auto composite_type = call->arguments()[0]->type()->As<ast::StructType>();
  auto field_name = call->arguments()[1]->As<ast::IdentifierExpr>();
  const uint32_t offset = composite_type->GetOffsetOfFieldByName(field_name->name());
  LocalVar offset_var = GetExecutionResult()->GetOrCreateDestination(call->type());
  GetEmitter()->EmitAssignImm4(offset_var, offset);
  GetExecutionResult()->SetDestination(offset_var.ValueOf());
}

void BytecodeGenerator::VisitBuiltinCallExpr(ast::CallExpr *call) {
  ast::Builtin builtin;

  ast::Context *ctx = call->type()->context();
  ctx->IsBuiltinFunction(call->GetFuncName(), &builtin);

  switch (builtin) {
    case ast::Builtin::BoolToSql:
    case ast::Builtin::IntToSql:
    case ast::Builtin::FloatToSql:
    case ast::Builtin::DateToSql:
    case ast::Builtin::StringToSql:
    case ast::Builtin::SqlToBool: {
      VisitSqlConversionCall(call, builtin);
      break;
    }
    case ast::Builtin::Like: {
      VisitSqlStringLikeCall(call);
      break;
    }
    case ast::Builtin::ExecutionContextGetMemoryPool: {
      VisitExecutionContextCall(call, builtin);
      break;
    }
    case ast::Builtin::ThreadStateContainerInit:
    case ast::Builtin::ThreadStateContainerIterate:
    case ast::Builtin::ThreadStateContainerReset:
    case ast::Builtin::ThreadStateContainerFree: {
      VisitBuiltinThreadStateContainerCall(call, builtin);
      break;
    }
    case ast::Builtin::TableIterInit:
    case ast::Builtin::TableIterAdvance:
    case ast::Builtin::TableIterGetVPI:
    case ast::Builtin::TableIterClose: {
      VisitBuiltinTableIterCall(call, builtin);
      break;
    }
    case ast::Builtin::TableIterParallel: {
      VisitBuiltinTableIterParallelCall(call);
      break;
    }
    case ast::Builtin::VPIInit:
    case ast::Builtin::VPIFree:
    case ast::Builtin::VPIIsFiltered:
    case ast::Builtin::VPIGetSelectedRowCount:
    case ast::Builtin::VPIGetVectorProjection:
    case ast::Builtin::VPIHasNext:
    case ast::Builtin::VPIHasNextFiltered:
    case ast::Builtin::VPIAdvance:
    case ast::Builtin::VPIAdvanceFiltered:
    case ast::Builtin::VPISetPosition:
    case ast::Builtin::VPISetPositionFiltered:
    case ast::Builtin::VPIMatch:
    case ast::Builtin::VPIReset:
    case ast::Builtin::VPIResetFiltered:
    case ast::Builtin::VPIGetSmallInt:
    case ast::Builtin::VPIGetInt:
    case ast::Builtin::VPIGetBigInt:
    case ast::Builtin::VPIGetReal:
    case ast::Builtin::VPIGetDouble:
    case ast::Builtin::VPIGetDate:
    case ast::Builtin::VPIGetString:
    case ast::Builtin::VPISetSmallInt:
    case ast::Builtin::VPISetInt:
    case ast::Builtin::VPISetBigInt:
    case ast::Builtin::VPISetReal:
    case ast::Builtin::VPISetDouble:
    case ast::Builtin::VPISetDate:
    case ast::Builtin::VPISetString: {
      VisitBuiltinVPICall(call, builtin);
      break;
    }
    case ast::Builtin::Hash: {
      VisitBuiltinHashCall(call);
      break;
    };
    case ast::Builtin::FilterManagerInit:
    case ast::Builtin::FilterManagerInsertFilter:
    case ast::Builtin::FilterManagerFinalize:
    case ast::Builtin::FilterManagerRunFilters:
    case ast::Builtin::FilterManagerFree: {
      VisitBuiltinFilterManagerCall(call, builtin);
      break;
    }
    case ast::Builtin::VectorFilterEqual:
    case ast::Builtin::VectorFilterGreaterThan:
    case ast::Builtin::VectorFilterGreaterThanEqual:
    case ast::Builtin::VectorFilterLessThan:
    case ast::Builtin::VectorFilterLessThanEqual:
    case ast::Builtin::VectorFilterNotEqual: {
      VisitBuiltinVectorFilterCall(call, builtin);
      break;
    }
    case ast::Builtin::AggHashTableInit:
    case ast::Builtin::AggHashTableInsert:
    case ast::Builtin::AggHashTableLinkEntry:
    case ast::Builtin::AggHashTableLookup:
    case ast::Builtin::AggHashTableProcessBatch:
    case ast::Builtin::AggHashTableMovePartitions:
    case ast::Builtin::AggHashTableParallelPartitionedScan:
    case ast::Builtin::AggHashTableFree: {
      VisitBuiltinAggHashTableCall(call, builtin);
      break;
    }
    case ast::Builtin::AggPartIterHasNext:
    case ast::Builtin::AggPartIterNext:
    case ast::Builtin::AggPartIterGetRow:
    case ast::Builtin::AggPartIterGetRowEntry:
    case ast::Builtin::AggPartIterGetHash: {
      VisitBuiltinAggPartIterCall(call, builtin);
      break;
    }
    case ast::Builtin::AggHashTableIterInit:
    case ast::Builtin::AggHashTableIterHasNext:
    case ast::Builtin::AggHashTableIterNext:
    case ast::Builtin::AggHashTableIterGetRow:
    case ast::Builtin::AggHashTableIterClose: {
      VisitBuiltinAggHashTableIterCall(call, builtin);
      break;
    }
    case ast::Builtin::AggInit:
    case ast::Builtin::AggAdvance:
    case ast::Builtin::AggMerge:
    case ast::Builtin::AggReset:
    case ast::Builtin::AggResult: {
      VisitBuiltinAggregatorCall(call, builtin);
      break;
    }
    case ast::Builtin::JoinHashTableInit:
    case ast::Builtin::JoinHashTableInsert:
    case ast::Builtin::JoinHashTableBuild:
    case ast::Builtin::JoinHashTableBuildParallel:
    case ast::Builtin::JoinHashTableLookup:
    case ast::Builtin::JoinHashTableFree: {
      VisitBuiltinJoinHashTableCall(call, builtin);
      break;
    }
    case ast::Builtin::HashTableEntryIterHasNext:
    case ast::Builtin::HashTableEntryIterGetRow: {
      VisitBuiltinHashTableEntryIteratorCall(call, builtin);
      break;
    }
    case ast::Builtin::SorterInit:
    case ast::Builtin::SorterInsert:
    case ast::Builtin::SorterInsertTopK:
    case ast::Builtin::SorterInsertTopKFinish:
    case ast::Builtin::SorterSort:
    case ast::Builtin::SorterSortParallel:
    case ast::Builtin::SorterSortTopKParallel:
    case ast::Builtin::SorterFree: {
      VisitBuiltinSorterCall(call, builtin);
      break;
    }
    case ast::Builtin::SorterIterInit:
    case ast::Builtin::SorterIterHasNext:
    case ast::Builtin::SorterIterNext:
    case ast::Builtin::SorterIterGetRow:
    case ast::Builtin::SorterIterClose: {
      VisitBuiltinSorterIterCall(call, builtin);
      break;
    }
    case ast::Builtin::ResultBufferAllocOutRow:
    case ast::Builtin::ResultBufferFinalize: {
      VisitResultBufferCall(call, builtin);
      break;
    }
    case ast::Builtin::ACos:
    case ast::Builtin::ASin:
    case ast::Builtin::ATan:
    case ast::Builtin::ATan2:
    case ast::Builtin::Cos:
    case ast::Builtin::Cot:
    case ast::Builtin::Sin:
    case ast::Builtin::Tan: {
      VisitBuiltinTrigCall(call, builtin);
      break;
    }
    case ast::Builtin::SizeOf: {
      VisitBuiltinSizeOfCall(call);
      break;
    }
    case ast::Builtin::OffsetOf: {
      VisitBuiltinOffsetOfCall(call);
      break;
    }
    case ast::Builtin::PtrCast: {
      Visit(call->arguments()[1]);
      break;
    }
  }
}

void BytecodeGenerator::VisitRegularCallExpr(ast::CallExpr *call) {
  bool caller_wants_result = GetExecutionResult() != nullptr;
  TPL_ASSERT(!caller_wants_result || GetExecutionResult()->IsRValue(),
             "Calls can only be R-Values!");

  std::vector<LocalVar> params;

  auto *func_type = call->function()->type()->As<ast::FunctionType>();

  if (!func_type->return_type()->IsNilType()) {
    LocalVar ret_val;
    if (caller_wants_result) {
      ret_val = GetExecutionResult()->GetOrCreateDestination(func_type->return_type());

      // Let the caller know where the result value is
      GetExecutionResult()->SetDestination(ret_val.ValueOf());
    } else {
      ret_val = GetCurrentFunction()->NewLocal(func_type->return_type());
    }

    // Push return value address into parameter list
    params.push_back(ret_val);
  }

  // Collect non-return-value parameters as usual
  for (uint32_t i = 0; i < func_type->num_params(); i++) {
    params.push_back(VisitExpressionForRValue(call->arguments()[i]));
  }

  // Emit call
  const auto func_id = LookupFuncIdByName(call->GetFuncName().data());
  TPL_ASSERT(func_id != FunctionInfo::kInvalidFuncId, "Function not found!");
  GetEmitter()->EmitCall(func_id, params);
}

void BytecodeGenerator::VisitCallExpr(ast::CallExpr *node) {
  ast::CallExpr::CallKind call_kind = node->call_kind();

  if (call_kind == ast::CallExpr::CallKind::Builtin) {
    VisitBuiltinCallExpr(node);
  } else {
    VisitRegularCallExpr(node);
  }
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
  TPL_ASSERT(GetExecutionResult()->IsRValue(), "Literal expressions cannot be R-Values!");

  LocalVar target = GetExecutionResult()->GetOrCreateDestination(node->type());

  switch (node->literal_kind()) {
    case ast::LitExpr::LitKind::Nil: {
      // Do nothing
      break;
    }
    case ast::LitExpr::LitKind::Boolean: {
      GetEmitter()->EmitAssignImm1(target, static_cast<int8_t>(node->bool_val()));
      break;
    }
    case ast::LitExpr::LitKind::Int: {
      GetEmitter()->EmitAssignImm4(target, node->int32_val());
      break;
    }
    case ast::LitExpr::LitKind::Float: {
      GetEmitter()->EmitAssignImm4F(target, node->float32_val());
      break;
    }
    default: {
      LOG_ERROR("Non-bool or non-integer literals not supported in bytecode");
      break;
    }
  }

  GetExecutionResult()->SetDestination(target.ValueOf());
}

void BytecodeGenerator::VisitStructDecl(UNUSED ast::StructDecl *node) {
  // Nothing to do
}

void BytecodeGenerator::VisitLogicalAndOrExpr(ast::BinaryOpExpr *node) {
  TPL_ASSERT(GetExecutionResult()->IsRValue(), "Binary expressions must be R-Values!");
  TPL_ASSERT(node->type()->IsBoolType(), "Boolean binary operation must be of type bool");

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->type());

  // Execute left child
  VisitExpressionForRValue(node->left(), dest);

  Bytecode conditional_jump;
  BytecodeLabel end_label;

  switch (node->op()) {
    case parsing::Token::Type::OR: {
      conditional_jump = Bytecode::JumpIfTrue;
      break;
    }
    case parsing::Token::Type::AND: {
      conditional_jump = Bytecode::JumpIfFalse;
      break;
    }
    default: { UNREACHABLE("Impossible logical operation type"); }
  }

  // Do a conditional jump
  GetEmitter()->EmitConditionalJump(conditional_jump, dest.ValueOf(), &end_label);

  // Execute the right child
  VisitExpressionForRValue(node->right(), dest);

  // Bind the end label
  GetEmitter()->Bind(&end_label);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitPrimitiveArithmeticExpr(ast::BinaryOpExpr *node) {
  TPL_ASSERT(GetExecutionResult()->IsRValue(), "Arithmetic expressions must be R-Values!");

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->type());
  LocalVar left = VisitExpressionForRValue(node->left());
  LocalVar right = VisitExpressionForRValue(node->right());

  Bytecode bytecode;
  switch (node->op()) {
    case parsing::Token::Type::PLUS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Add), node->type());
      break;
    }
    case parsing::Token::Type::MINUS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Sub), node->type());
      break;
    }
    case parsing::Token::Type::STAR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Mul), node->type());
      break;
    }
    case parsing::Token::Type::SLASH: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Div), node->type());
      break;
    }
    case parsing::Token::Type::PERCENT: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Rem), node->type());
      break;
    }
    case parsing::Token::Type::AMPERSAND: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitAnd), node->type());
      break;
    }
    case parsing::Token::Type::BIT_OR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitOr), node->type());
      break;
    }
    case parsing::Token::Type::BIT_XOR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitXor), node->type());
      break;
    }
    default: { UNREACHABLE("Impossible binary operation"); }
  }

  // Emit
  GetEmitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitSqlArithmeticExpr(ast::BinaryOpExpr *node) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->type());
  LocalVar left = VisitExpressionForLValue(node->left());
  LocalVar right = VisitExpressionForLValue(node->right());

  const bool is_integer_math = node->type()->IsSpecificBuiltin(ast::BuiltinType::Integer);

  Bytecode bytecode;
  switch (node->op()) {
    case parsing::Token::Type::PLUS: {
      bytecode = (is_integer_math ? Bytecode::AddInteger : Bytecode::AddReal);
      break;
    }
    case parsing::Token::Type::MINUS: {
      bytecode = (is_integer_math ? Bytecode::SubInteger : Bytecode::SubReal);
      break;
    }
    case parsing::Token::Type::STAR: {
      bytecode = (is_integer_math ? Bytecode::MulInteger : Bytecode::MulReal);
      break;
    }
    case parsing::Token::Type::SLASH: {
      bytecode = (is_integer_math ? Bytecode::DivInteger : Bytecode::DivReal);
      break;
    }
    case parsing::Token::Type::PERCENT: {
      bytecode = (is_integer_math ? Bytecode::RemInteger : Bytecode::RemReal);
      break;
    }
    default: { UNREACHABLE("Impossible arithmetic SQL operation"); }
  }

  // Emit
  GetEmitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest);
}

void BytecodeGenerator::VisitArithmeticExpr(ast::BinaryOpExpr *node) {
  if (node->type()->IsSqlValueType()) {
    VisitSqlArithmeticExpr(node);
  } else {
    VisitPrimitiveArithmeticExpr(node);
  }
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

#define SQL_COMPARISON_BYTECODE(CODE_RESULT, COMPARISON_TYPE, ARG_KIND) \
  switch (ARG_KIND) {                                                   \
    case ast::BuiltinType::Kind::Integer:                               \
      CODE_RESULT = Bytecode::COMPARISON_TYPE##Integer;                 \
      break;                                                            \
    case ast::BuiltinType::Kind::Real:                                  \
      CODE_RESULT = Bytecode::COMPARISON_TYPE##Real;                    \
      break;                                                            \
    case ast::BuiltinType::Kind::Date:                                  \
      CODE_RESULT = Bytecode::COMPARISON_TYPE##Date;                    \
      break;                                                            \
    case ast::BuiltinType::Kind::StringVal:                             \
      CODE_RESULT = Bytecode::COMPARISON_TYPE##String;                  \
      break;                                                            \
    default:                                                            \
      UNREACHABLE("Undefined SQL comparison!");                         \
  }

void BytecodeGenerator::VisitSqlCompareOpExpr(ast::ComparisonOpExpr *compare) {
  TPL_ASSERT(GetExecutionResult()->IsRValue(), "SQL comparison expressions must be R-Values!");

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(compare->type());
  LocalVar left = VisitExpressionForLValue(compare->left());
  LocalVar right = VisitExpressionForLValue(compare->right());

  TPL_ASSERT(compare->left()->type() == compare->right()->type(),
             "Left and right input types to comparison are not equal");

  const auto arg_kind = compare->left()->type()->As<ast::BuiltinType>()->kind();

  Bytecode code;
  switch (compare->op()) {
    case parsing::Token::Type::GREATER: {
      SQL_COMPARISON_BYTECODE(code, GreaterThan, arg_kind);
      break;
    }
    case parsing::Token::Type::GREATER_EQUAL: {
      SQL_COMPARISON_BYTECODE(code, GreaterThanEqual, arg_kind);
      break;
    }
    case parsing::Token::Type::EQUAL_EQUAL: {
      SQL_COMPARISON_BYTECODE(code, Equal, arg_kind);
      break;
    }
    case parsing::Token::Type::LESS: {
      SQL_COMPARISON_BYTECODE(code, LessThan, arg_kind);
      break;
    }
    case parsing::Token::Type::LESS_EQUAL: {
      SQL_COMPARISON_BYTECODE(code, LessThanEqual, arg_kind);
      break;
    }
    case parsing::Token::Type::BANG_EQUAL: {
      SQL_COMPARISON_BYTECODE(code, NotEqual, arg_kind);
      break;
    }
    default: { UNREACHABLE("Impossible binary operation"); }
  }

  // Emit
  GetEmitter()->EmitBinaryOp(code, dest, left, right);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest);
}

#undef SQL_COMPARISON_BYTECODE

void BytecodeGenerator::VisitPrimitiveCompareOpExpr(ast::ComparisonOpExpr *compare) {
  TPL_ASSERT(GetExecutionResult()->IsRValue(), "Comparison expressions must be R-Values!");

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(compare->type());

  // nil comparison
  if (ast::Expr * input_expr; compare->IsLiteralCompareNil(&input_expr)) {
    LocalVar input = VisitExpressionForRValue(input_expr);
    Bytecode bytecode = compare->op() == parsing::Token::Type ::EQUAL_EQUAL
                            ? Bytecode::IsNullPtr
                            : Bytecode::IsNotNullPtr;
    GetEmitter()->Emit(bytecode, dest, input);
    GetExecutionResult()->SetDestination(dest.ValueOf());
    return;
  }

  // regular comparison

  LocalVar left = VisitExpressionForRValue(compare->left());
  LocalVar right = VisitExpressionForRValue(compare->right());

  Bytecode bytecode;
  switch (compare->op()) {
    case parsing::Token::Type::GREATER: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::GreaterThan),
                                     compare->left()->type());
      break;
    }
    case parsing::Token::Type::GREATER_EQUAL: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::GreaterThanEqual),
                                     compare->left()->type());
      break;
    }
    case parsing::Token::Type::EQUAL_EQUAL: {
      bytecode =
          GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Equal), compare->left()->type());
      break;
    }
    case parsing::Token::Type::LESS: {
      bytecode =
          GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::LessThan), compare->left()->type());
      break;
    }
    case parsing::Token::Type::LESS_EQUAL: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::LessThanEqual),
                                     compare->left()->type());
      break;
    }
    case parsing::Token::Type::BANG_EQUAL: {
      bytecode =
          GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::NotEqual), compare->left()->type());
      break;
    }
    default: { UNREACHABLE("Impossible binary operation"); }
  }

  // Emit
  GetEmitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitComparisonOpExpr(ast::ComparisonOpExpr *node) {
  TPL_ASSERT(GetExecutionResult()->IsRValue(), "Comparison expressions must be R-Values!");

  const bool is_primitive_comparison = node->type()->IsSpecificBuiltin(ast::BuiltinType::Bool);

  if (!is_primitive_comparison) {
    VisitSqlCompareOpExpr(node);
  } else {
    VisitPrimitiveCompareOpExpr(node);
  }
}

void BytecodeGenerator::VisitFunctionLitExpr(ast::FunctionLitExpr *node) { Visit(node->body()); }

void BytecodeGenerator::BuildAssign(LocalVar dest, LocalVar val, ast::Type *dest_type) {
  // Emit the appropriate assignment
  const uint32_t size = dest_type->size();
  if (size == 1) {
    GetEmitter()->EmitAssign(Bytecode::Assign1, dest, val);
  } else if (size == 2) {
    GetEmitter()->EmitAssign(Bytecode::Assign2, dest, val);
  } else if (size == 4) {
    GetEmitter()->EmitAssign(Bytecode::Assign4, dest, val);
  } else {
    GetEmitter()->EmitAssign(Bytecode::Assign8, dest, val);
  }
}

void BytecodeGenerator::BuildDeref(LocalVar dest, LocalVar ptr, ast::Type *dest_type) {
  // Emit the appropriate deref
  const uint32_t size = dest_type->size();
  if (size == 1) {
    GetEmitter()->EmitDeref(Bytecode::Deref1, dest, ptr);
  } else if (size == 2) {
    GetEmitter()->EmitDeref(Bytecode::Deref2, dest, ptr);
  } else if (size == 4) {
    GetEmitter()->EmitDeref(Bytecode::Deref4, dest, ptr);
  } else if (size == 8) {
    GetEmitter()->EmitDeref(Bytecode::Deref8, dest, ptr);
  } else {
    GetEmitter()->EmitDerefN(dest, ptr, size);
  }
}

LocalVar BytecodeGenerator::BuildLoadPointer(LocalVar double_ptr, ast::Type *type) {
  if (double_ptr.GetAddressMode() == LocalVar::AddressMode::Address) {
    return double_ptr.ValueOf();
  }

  // Need to Deref
  LocalVar ptr = GetCurrentFunction()->NewLocal(type);
  GetEmitter()->EmitDeref(Bytecode::Deref8, ptr, double_ptr);
  return ptr.ValueOf();
}

void BytecodeGenerator::VisitMemberExpr(ast::MemberExpr *node) {
  // We first need to compute the address of the object we're selecting into.
  // Thus, we get the L-Value of the object below.

  LocalVar obj_ptr = VisitExpressionForLValue(node->object());

  // We now need to compute the offset of the field in the composite type. TPL
  // unifies C's arrow and dot syntax for field/member access. Thus, the type
  // of the object may be either a pointer to a struct or the actual struct. If
  // the type is a pointer, then the L-Value of the object is actually a double
  // pointer and we need to dereference it; otherwise, we can use the address
  // as is.

  ast::StructType *obj_type = nullptr;
  if (auto *type = node->object()->type(); node->IsSugaredArrow()) {
    // Double pointer, need to dereference
    obj_ptr = BuildLoadPointer(obj_ptr, type);
    obj_type = type->As<ast::PointerType>()->base()->As<ast::StructType>();
  } else {
    obj_type = type->As<ast::StructType>();
  }

  // We're now ready to compute offset. Let's lookup the field's offset in the
  // struct type.

  auto *field_name = node->member()->As<ast::IdentifierExpr>();
  auto offset = obj_type->GetOffsetOfFieldByName(field_name->name());

  // Now that we have a pointer to the composite object, we need to compute a
  // pointer to the field within the object. If the offset of the field in the
  // object is zero, we needn't do anything - we can just reinterpret the object
  // pointer. If the field offset is greater than zero, we generate a LEA.

  LocalVar field_ptr;
  if (offset == 0) {
    field_ptr = obj_ptr;
  } else {
    field_ptr = GetCurrentFunction()->NewLocal(node->type()->PointerTo());
    GetEmitter()->EmitLea(field_ptr, obj_ptr, offset);
    field_ptr = field_ptr.ValueOf();
  }

  if (GetExecutionResult()->IsLValue()) {
    TPL_ASSERT(!GetExecutionResult()->HasDestination(), "L-Values produce their destination");
    GetExecutionResult()->SetDestination(field_ptr);
    return;
  }

  // The caller wants the actual value of the field. We just computed a pointer
  // to the field in the object, so we need to load/dereference it. If the
  // caller provided a destination variable, use that; otherwise, create a new
  // temporary variable to store the value.

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->type());
  BuildDeref(dest, field_ptr, node->type());
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitDeclStmt(ast::DeclStmt *node) { Visit(node->declaration()); }

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

FunctionInfo *BytecodeGenerator::AllocateFunc(const std::string &func_name,
                                              ast::FunctionType *const func_type) {
  // Allocate function
  const auto func_id = static_cast<FunctionId>(functions_.size());
  functions_.emplace_back(func_id, func_name, func_type);
  FunctionInfo *func = &functions_.back();

  // Register return type
  if (auto *return_type = func_type->return_type(); !return_type->IsNilType()) {
    func->NewParameterLocal(return_type->PointerTo(), "hiddenRv");
  }

  // Register parameters
  for (const auto &param : func_type->params()) {
    func->NewParameterLocal(param.type, param.name.data());
  }

  // Cache
  func_map_[func->GetName()] = func->GetId();

  return func;
}

FunctionId BytecodeGenerator::LookupFuncIdByName(const std::string &name) const {
  auto iter = func_map_.find(name);
  if (iter == func_map_.end()) {
    return FunctionInfo::kInvalidFuncId;
  }
  return iter->second;
}

LocalVar BytecodeGenerator::NewStatic(const std::string &name, ast::Type *type,
                                      const void *contents) {
  std::size_t offset = data_.size();

  if (!util::MathUtil::IsAligned(offset, type->alignment())) {
    offset = util::MathUtil::AlignTo(offset, type->alignment());
  }

  const std::size_t padded_len = type->size() + (offset - data_.size());
  data_.insert(data_.end(), padded_len, 0);
  std::memcpy(&data_[offset], contents, type->size());

  uint32_t &version = static_locals_versions_[name];
  const std::string name_and_version = name + "_" + std::to_string(version++);
  static_locals_.emplace_back(name_and_version, type, offset, LocalInfo::Kind::Var);

  return LocalVar(offset, LocalVar::AddressMode::Address);
}

LocalVar BytecodeGenerator::NewStaticString(ast::Context *ctx, const ast::Identifier string) {
  // Check cache
  if (auto iter = static_string_cache_.find(string); iter != static_string_cache_.end()) {
    return LocalVar(iter->second.GetOffset(), LocalVar::AddressMode::Address);
  }

  // Create
  auto *type =
      ast::ArrayType::Get(string.length(), ast::BuiltinType::Get(ctx, ast::BuiltinType::Uint8));
  auto static_local = NewStatic("stringConst", type, static_cast<const void *>(string.data()));

  // Cache
  static_string_cache_.emplace(string, static_local);

  return static_local;
}

LocalVar BytecodeGenerator::VisitExpressionForLValue(ast::Expr *expr) {
  LValueResultScope scope(this);
  Visit(expr);
  return scope.GetDestination();
}

LocalVar BytecodeGenerator::VisitExpressionForRValue(ast::Expr *expr) {
  RValueResultScope scope(this);
  Visit(expr);
  return scope.GetDestination();
}

void BytecodeGenerator::VisitExpressionForRValue(ast::Expr *expr, LocalVar dest) {
  RValueResultScope scope(this, dest);
  Visit(expr);
}

void BytecodeGenerator::VisitExpressionForTest(ast::Expr *expr, BytecodeLabel *then_label,
                                               BytecodeLabel *else_label,
                                               TestFallthrough fallthrough) {
  // Evaluate the expression
  LocalVar cond = VisitExpressionForRValue(expr);

  switch (fallthrough) {
    case TestFallthrough::Then: {
      GetEmitter()->EmitConditionalJump(Bytecode::JumpIfFalse, cond, else_label);
      break;
    }
    case TestFallthrough::Else: {
      GetEmitter()->EmitConditionalJump(Bytecode::JumpIfTrue, cond, then_label);
      break;
    }
    case TestFallthrough::None: {
      GetEmitter()->EmitConditionalJump(Bytecode::JumpIfFalse, cond, else_label);
      GetEmitter()->EmitJump(Bytecode::Jump, then_label);
      break;
    }
  }
}

Bytecode BytecodeGenerator::GetIntTypedBytecode(Bytecode bytecode, ast::Type *type) {
  TPL_ASSERT(type->IsIntegerType(), "Type must be integer type");
  auto int_kind = type->SafeAs<ast::BuiltinType>()->kind();
  auto kind_idx = static_cast<uint8_t>(int_kind - ast::BuiltinType::Int8);
  return Bytecodes::FromByte(Bytecodes::ToByte(bytecode) + kind_idx);
}

// static
std::unique_ptr<BytecodeModule> BytecodeGenerator::Compile(ast::AstNode *root,
                                                           const std::string &name) {
  BytecodeGenerator generator;
  generator.Visit(root);

  // Create the bytecode module. Note that we move the bytecode and functions
  // array from the generator into the module.
  return std::make_unique<BytecodeModule>(
      name, std::move(generator.code_), std::move(generator.data_), std::move(generator.functions_),
      std::move(generator.static_locals_));
}

}  // namespace tpl::vm

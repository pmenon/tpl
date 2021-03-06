#include "vm/bytecode_generator.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "spdlog/fmt/fmt.h"

#include "ast/builtins.h"
#include "ast/context.h"
#include "ast/type.h"
#include "common/exception.h"
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
  ExpressionResultScope(BytecodeGenerator *generator, ast::Expression::Context kind,
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
  bool IsLValue() const { return kind_ == ast::Expression::Context::LValue; }

  /**
   * @return True if the expression is an R-Value expression; false otherwise.
   */
  bool IsRValue() const { return kind_ == ast::Expression::Context::RValue; }

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
  ast::Expression::Context kind_;
};

/**
 * An expression result scope that indicates the result is used as an L-Value.
 */
class BytecodeGenerator::LValueResultScope : public BytecodeGenerator::ExpressionResultScope {
 public:
  explicit LValueResultScope(BytecodeGenerator *generator, LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expression::Context::LValue, dest) {}
};

/**
 * An expression result scope that indicates the result is used as an R-Value.
 */
class BytecodeGenerator::RValueResultScope : public BytecodeGenerator::ExpressionResultScope {
 public:
  explicit RValueResultScope(BytecodeGenerator *generator, LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expression::Context::RValue, dest) {}
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
    func_->SetBytecodeRange(start_offset_, end_offset);
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

void BytecodeGenerator::VisitIfStatement(ast::IfStatement *node) {
  IfThenElseBuilder if_builder(this);

  // Generate condition check code
  VisitExpressionForTest(node->GetCondition(), if_builder.GetThenLabel(), if_builder.GetElseLabel(),
                         TestFallthrough::Then);

  // Generate code in "then" block
  if_builder.Then();
  Visit(node->GetThenStatement());

  // If there's an "else" block, handle it now
  if (node->GetElseStatement() != nullptr) {
    if (!ast::Statement::IsTerminating(node->GetThenStatement())) {
      if_builder.JumpToEnd();
    }
    if_builder.Else();
    Visit(node->GetElseStatement());
  }
}

void BytecodeGenerator::VisitIterationStatement(ast::IterationStatement *iteration,
                                                LoopBuilder *loop_builder) {
  Visit(iteration->GetBody());
  loop_builder->BindContinueTarget();
}

void BytecodeGenerator::VisitForStatement(ast::ForStatement *node) {
  LoopBuilder loop_builder(this);

  if (node->GetInit() != nullptr) {
    Visit(node->GetInit());
  }

  loop_builder.LoopHeader();

  if (node->GetCondition() != nullptr) {
    BytecodeLabel loop_body_label;
    VisitExpressionForTest(node->GetCondition(), &loop_body_label, loop_builder.GetBreakLabel(),
                           TestFallthrough::Then);
  }

  VisitIterationStatement(node, &loop_builder);

  if (node->GetNext() != nullptr) {
    Visit(node->GetNext());
  }

  loop_builder.JumpToHeader();
}

void BytecodeGenerator::VisitForInStatement(UNUSED ast::ForInStatement *node) {
  TPL_ASSERT(false, "For-in statements not supported");
}

void BytecodeGenerator::VisitFieldDeclaration(ast::FieldDeclaration *node) {
  AstVisitor::VisitFieldDeclaration(node);
}

void BytecodeGenerator::VisitFunctionDeclaration(ast::FunctionDeclaration *node) {
  // The function's TPL type
  auto *func_type = node->GetTypeRepr()->GetType()->As<ast::FunctionType>();

  // Allocate the function
  FunctionInfo *func_info = AllocateFunc(node->GetName().ToString(), func_type);

  {
    // Visit the body of the function. We use this handy scope object to track
    // the start and end position of this function's bytecode in the module's
    // bytecode array. Upon destruction, the scoped class will set the bytecode
    // range in the function.
    BytecodePositionScope position_scope(this, func_info);
    Visit(node->GetFunctionLiteral());
  }
}

void BytecodeGenerator::VisitIdentifierExpression(ast::IdentifierExpression *node) {
  TPL_ASSERT(GetExecutionResult() != nullptr,
             "Caller expected to use result of identifier expression.");

  // Lookup the local in the current function. It must be there through a
  // previous variable declaration (or parameter declaration). What is returned
  // is a pointer to the variable.
  std::string_view local_name = node->GetName().GetView();
  LocalVar local = GetCurrentFunction()->LookupLocal(local_name);

  // If the caller wants the L-Value of the identifier, we just provide the
  // local we found, which is a register with the address of the identifier.
  if (GetExecutionResult()->IsLValue()) {
    GetExecutionResult()->SetDestination(local);
    return;
  }

  // The caller wants the R-Value of the identifier, so we need to load it. If
  // the caller did not provide a destination register, we're done.
  if (!GetExecutionResult()->HasDestination()) {
    GetExecutionResult()->SetDestination(local.ValueOf());
    return;
  }

  // The caller wants the R-Value of the identifier and provided a destination.
  // Thus, we need to copy the identifier's value into the provided destination.
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());

  // If the local we want the R-Value of is a parameter, we can't take its
  // pointer for the deref, so we use an assignment. Otherwise, a deref is good.
  if (auto *local_info = GetCurrentFunction()->LookupLocalInfoByName(local_name);
      local_info->IsParameter()) {
    BuildAssign(dest, local.ValueOf(), node->GetType());
  } else {
    BuildDeref(dest, local, node->GetType());
  }

  GetExecutionResult()->SetDestination(dest);
}

void BytecodeGenerator::VisitImplicitCastExpression(ast::ImplicitCastExpression *node) {
  switch (node->GetCastKind()) {
    case ast::CastKind::SqlBoolToBool: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
      LocalVar input = VisitExpressionForSQLValue(node->GetInput());
      GetEmitter()->Emit(Bytecode::ForceBoolTruth, dest, input);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::CastKind::BoolToSqlBool: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
      LocalVar input = VisitExpressionForRValue(node->GetInput());
      GetEmitter()->Emit(Bytecode::InitBool, dest, input);
      break;
    }
    case ast::CastKind::IntToSqlInt: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
      LocalVar input = VisitExpressionForRValue(node->GetInput());
      GetEmitter()->Emit(Bytecode::InitInteger, dest, input);
      break;
    }
    case ast::CastKind::BitCast:
    case ast::CastKind::IntegralCast: {
      // As an optimization, we only issue a new assignment if the input and
      // output types of the cast have different sizes.
      LocalVar input = VisitExpressionForRValue(node->GetInput());
      if (node->GetInput()->GetType()->GetSize() != node->GetType()->GetSize()) {
        LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
        BuildAssign(dest, input, node->GetType());
        GetExecutionResult()->SetDestination(dest.ValueOf());
      } else {
        GetExecutionResult()->SetDestination(input);
      }
      break;
    }
    case ast::CastKind::FloatToSqlReal: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
      LocalVar input = VisitExpressionForRValue(node->GetInput());
      GetEmitter()->Emit(Bytecode::InitReal, dest, input);
      break;
    }
    case ast::CastKind::SqlIntToSqlReal: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
      LocalVar input = VisitExpressionForSQLValue(node->GetInput());
      GetEmitter()->Emit(Bytecode::IntegerToReal, dest, input);
      break;
    }
    default: {
      throw NotImplementedException(
          fmt::format("'{}' cast is not implemented", ast::CastKindToString(node->GetCastKind())));
    }
  }
}

void BytecodeGenerator::VisitArrayIndexExpression(ast::IndexExpression *node) {
  // The type and the element's size
  auto type = node->GetObject()->GetType()->As<ast::ArrayType>();
  auto elem_size = type->GetElementType()->GetSize();

  // First, we need to get the base address of the array
  LocalVar arr;
  if (type->HasKnownLength()) {
    arr = VisitExpressionForLValue(node->GetObject());
  } else {
    arr = VisitExpressionForRValue(node->GetObject());
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

  LocalVar elem_ptr = GetCurrentFunction()->NewLocal(node->GetType()->PointerTo());

  if (node->GetIndex()->IsIntegerLiteral()) {
    const int64_t index = node->GetIndex()->As<ast::LiteralExpression>()->IntegerVal();
    TPL_ASSERT(index >= 0, "Array indexes must be non-negative! Should");
    GetEmitter()->EmitLea(elem_ptr, arr, (elem_size * index));
  } else {
    LocalVar index = VisitExpressionForRValue(node->GetIndex());
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

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
  BuildDeref(dest, elem_ptr, node->GetType());
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitMapIndexExpression(ast::IndexExpression *node) {}

void BytecodeGenerator::VisitIndexExpression(ast::IndexExpression *node) {
  if (node->GetObject()->GetType()->IsArrayType()) {
    VisitArrayIndexExpression(node);
  } else {
    VisitMapIndexExpression(node);
  }
}

void BytecodeGenerator::VisitBlockStatement(ast::BlockStatement *node) {
  for (auto *stmt : node->GetStatements()) {
    Visit(stmt);
  }
}

void BytecodeGenerator::VisitVariableDeclaration(ast::VariableDeclaration *node) {
  // Register a new local variable in the function. If the variable has an
  // explicit type specifier, prefer using that. Otherwise, use the type of the
  // initial value resolved after semantic analysis.
  ast::Type *type = nullptr;
  if (node->GetTypeRepr() != nullptr) {
    TPL_ASSERT(node->GetTypeRepr()->GetType() != nullptr,
               "Variable with explicit type declaration is missing resolved "
               "type at runtime!");
    type = node->GetTypeRepr()->GetType();
  } else {
    TPL_ASSERT(node->GetInitialValue() != nullptr,
               "Variable without explicit type declaration is missing an "
               "initialization expression!");
    TPL_ASSERT(node->GetInitialValue()->GetType() != nullptr,
               "Variable with initial value is missing resolved type");
    type = node->GetInitialValue()->GetType();
  }

  // Register this variable in the function as a local
  LocalVar local = GetCurrentFunction()->NewLocal(type, node->GetName().ToString());

  // If there's an initializer, generate code for it now
  if (node->GetInitialValue() != nullptr) {
    VisitExpressionForRValue(node->GetInitialValue(), local);
  }
}

void BytecodeGenerator::VisitAddressOfExpression(ast::UnaryOpExpression *op) {
  TPL_ASSERT(GetExecutionResult()->IsRValue(), "Address-of expressions must be R-values!");
  LocalVar addr = VisitExpressionForLValue(op->GetInput());
  if (GetExecutionResult()->HasDestination()) {
    LocalVar dest = GetExecutionResult()->GetDestination();
    BuildAssign(dest, addr, op->GetType());
  } else {
    GetExecutionResult()->SetDestination(addr);
  }
}

void BytecodeGenerator::VisitDerefExpression(ast::UnaryOpExpression *op) {
  LocalVar addr = VisitExpressionForRValue(op->GetInput());
  if (GetExecutionResult()->IsLValue()) {
    GetExecutionResult()->SetDestination(addr);
  } else {
    LocalVar dest = GetExecutionResult()->GetOrCreateDestination(op->GetType());
    BuildDeref(dest, addr, op->GetType());
    GetExecutionResult()->SetDestination(dest.ValueOf());
  }
}

void BytecodeGenerator::VisitArithmeticUnaryExpression(ast::UnaryOpExpression *op) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(op->GetType());
  LocalVar input = VisitExpressionForRValue(op->GetInput());

  Bytecode bytecode;
  switch (op->Op()) {
    case parsing::Token::Type::MINUS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Neg), op->GetType());
      break;
    }
    case parsing::Token::Type::BIT_NOT: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitNeg), op->GetType());
      break;
    }
    default: {
      UNREACHABLE("Impossible unary operation");
    }
  }

  // Emit
  GetEmitter()->EmitUnaryOp(bytecode, dest, input);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitLogicalNotExpression(ast::UnaryOpExpression *op) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(op->GetType());
  LocalVar input = VisitExpressionForRValue(op->GetInput());
  GetEmitter()->EmitUnaryOp(Bytecode::Not, dest, input);
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitUnaryOpExpression(ast::UnaryOpExpression *node) {
  switch (node->Op()) {
    case parsing::Token::Type::AMPERSAND: {
      VisitAddressOfExpression(node);
      break;
    }
    case parsing::Token::Type::STAR: {
      VisitDerefExpression(node);
      break;
    }
    case parsing::Token::Type::MINUS:
    case parsing::Token::Type::BIT_NOT: {
      VisitArithmeticUnaryExpression(node);
      break;
    }
    case parsing::Token::Type::BANG: {
      VisitLogicalNotExpression(node);
      break;
    }
    default: {
      UNREACHABLE("Impossible unary operation");
    }
  }
}

void BytecodeGenerator::VisitReturnStatement(ast::ReturnStatement *node) {
  if (node->GetReturnValue() != nullptr) {
    LocalVar rv = GetCurrentFunction()->GetReturnValueLocal();
    // The return value 'rv' is the address of the return value in the frame.
    // In other words, it's a double pointer to the result. Thus, we need to
    // store the result of the recursive call into *rv, i.e., rv.ValueOf().
    TPL_ASSERT(rv.GetAddressMode() == LocalVar::AddressMode::Address, "RV expected to be address.");
    if (auto ret_type = node->GetReturnValue()->GetType(); ret_type->IsSqlValueType()) {
      LocalVar result = VisitExpressionForSQLValue(node->GetReturnValue());
      BuildDeref(rv.ValueOf(), result, ret_type);
    } else if (ret_type->IsStructType()) {
      LocalVar result = VisitExpressionForLValue(node->GetReturnValue());
      BuildAssign(rv.ValueOf(), result, ret_type);
    } else {
      LocalVar result = VisitExpressionForRValue(node->GetReturnValue());
      BuildAssign(rv.ValueOf(), result, ret_type);
    }
  }
  GetEmitter()->EmitReturn();
}

void BytecodeGenerator::VisitSqlConversionCall(ast::CallExpression *call, ast::Builtin builtin) {
  TPL_ASSERT(call->GetType() != nullptr, "No return type set for call!");

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());

  switch (builtin) {
    case ast::Builtin::BoolToSql: {
      auto input = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::InitBool, dest, input);
      break;
    }
    case ast::Builtin::IntToSql: {
      auto input = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::InitInteger, dest, input);
      break;
    }
    case ast::Builtin::FloatToSql: {
      auto input = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::InitReal, dest, input);
      break;
    }
    case ast::Builtin::DateToSql: {
      auto year = VisitExpressionForRValue(call->GetArguments()[0]);
      auto month = VisitExpressionForRValue(call->GetArguments()[1]);
      auto day = VisitExpressionForRValue(call->GetArguments()[2]);
      GetEmitter()->Emit(Bytecode::InitDate, dest, year, month, day);
      break;
    }
    case ast::Builtin::StringToSql: {
      auto string_lit = call->GetArguments()[0]->As<ast::LiteralExpression>()->StringVal();
      auto static_string = NewStaticString(call->GetType()->GetContext(), string_lit);
      GetEmitter()->EmitInitString(dest, static_string, string_lit.GetLength());
      break;
    }
    case ast::Builtin::SqlToBool: {
      auto input = VisitExpressionForSQLValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::ForceBoolTruth, dest, input);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }

#define GEN_CASE(Builtin, Bytecode)                                   \
  case Builtin: {                                                     \
    auto input = VisitExpressionForSQLValue(call->GetArguments()[0]); \
    GetEmitter()->Emit(Bytecode, dest, input);                        \
    break;                                                            \
  }
      GEN_CASE(ast::Builtin::ConvertBoolToInteger, Bytecode::BoolToInteger);
      GEN_CASE(ast::Builtin::ConvertIntegerToReal, Bytecode::IntegerToReal);
      GEN_CASE(ast::Builtin::ConvertDateToTimestamp, Bytecode::DateToTimestamp);
      GEN_CASE(ast::Builtin::ConvertStringToBool, Bytecode::StringToBool);
      GEN_CASE(ast::Builtin::ConvertStringToInt, Bytecode::StringToInteger);
      GEN_CASE(ast::Builtin::ConvertStringToReal, Bytecode::StringToReal);
      GEN_CASE(ast::Builtin::ConvertStringToDate, Bytecode::StringToDate);
      GEN_CASE(ast::Builtin::ConvertStringToTime, Bytecode::StringToTimestamp);
#undef GEN_CASE

    default: {
      UNREACHABLE("Impossible SQL conversion call");
    }
  }
}

void BytecodeGenerator::VisitNullValueCall(ast::CallExpression *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::IsValNull: {
      LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar input = VisitExpressionForSQLValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::ValIsNull, result, input);
      GetExecutionResult()->SetDestination(result.ValueOf());
      break;
    }
    case ast::Builtin::InitSqlNull: {
      LocalVar input = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::InitSqlNull, input);
      break;
    }
    default: {
      UNREACHABLE("VisitNullValueCall unknown builtin type.");
    }
  }
}

void BytecodeGenerator::VisitLNotCall(ast::CallExpression *call) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  LocalVar input = VisitExpressionForSQLValue(call->GetArguments()[0]);
  GetEmitter()->Emit(Bytecode::LogicalNotBoolVal, dest, input);
  GetExecutionResult()->SetDestination(dest);
}

void BytecodeGenerator::VisitSqlStringLikeCall(ast::CallExpression *call) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  LocalVar input = VisitExpressionForSQLValue(call->GetArguments()[0]);
  LocalVar pattern = VisitExpressionForSQLValue(call->GetArguments()[1]);
  GetEmitter()->Emit(Bytecode::Like, dest, input, pattern);
  GetExecutionResult()->SetDestination(dest);
}

void BytecodeGenerator::VisitBuiltinDateFunctionCall(ast::CallExpression *call,
                                                     ast::Builtin builtin) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  LocalVar input = VisitExpressionForSQLValue(call->GetArguments()[0]);

  switch (builtin) {
    case ast::Builtin::ExtractYear:
      GetEmitter()->Emit(Bytecode::ExtractYear, dest, input);
      break;
    default:
      UNREACHABLE("Impossible date call!");
  }
  GetExecutionResult()->SetDestination(dest);
}

void BytecodeGenerator::VisitBuiltinConcatCall(ast::CallExpression *call) {
  // TODO(pmenon): Should this be done earlier through an AST rewrite?
  auto dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  auto exec_ctx = VisitExpressionForRValue(call->GetArguments()[0]);

  // The number of input strings. Remember, the first parameter is context.
  const auto num_strings = call->GetArguments().size() - 1;

  // What we're doing here:
  // var arr: [N]*StringVal
  // arr[0] = input0
  // arr[1] = input1
  // ...
  // @concat(exec_ctx, arr, N)

  const auto string_type =
      ast::BuiltinType::Get(call->GetType()->GetContext(), ast::BuiltinType::Kind::StringVal);
  const auto arr =
      GetCurrentFunction()->NewLocal(ast::ArrayType::Get(num_strings, string_type->PointerTo()));

  // Populate array.
  auto arr_elem_ptr = GetCurrentFunction()->NewLocal(string_type->PointerTo()->PointerTo());
  for (std::size_t i = 0; i < num_strings; i++) {
    GetEmitter()->EmitLea(arr_elem_ptr, arr, i * 8);
    GetEmitter()->EmitAssign(Bytecode::Assign8, arr_elem_ptr.ValueOf(),
                             VisitExpressionForSQLValue(call->GetArguments()[i + 1]));
  }

  GetEmitter()->EmitConcat(dest, exec_ctx, arr, num_strings);
}

void BytecodeGenerator::VisitBuiltinTableIterCall(ast::CallExpression *call, ast::Builtin builtin) {
  // The first argument to all calls is a pointer to the TVI
  LocalVar iter = VisitExpressionForRValue(call->GetArguments()[0]);

  switch (builtin) {
    case ast::Builtin::TableIterInit: {
      // The second argument is the table name as a literal string
      TPL_ASSERT(call->GetArguments()[1]->IsStringLiteral(), "Table name must be a string literal");
      ast::Identifier table_name =
          call->GetArguments()[1]->As<ast::LiteralExpression>()->StringVal();
      sql::Table *table = sql::Catalog::Instance()->LookupTableByName(table_name);
      TPL_ASSERT(table != nullptr, "Table does not exist!");
      GetEmitter()->EmitTableIterInit(Bytecode::TableVectorIteratorInit, iter, table->GetId());
      GetEmitter()->Emit(Bytecode::TableVectorIteratorPerformInit, iter);
      break;
    }
    case ast::Builtin::TableIterAdvance: {
      LocalVar cond = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::TableVectorIteratorNext, cond, iter);
      GetExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::TableIterGetVPI: {
      LocalVar vpi = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::TableVectorIteratorGetVPI, vpi, iter);
      GetExecutionResult()->SetDestination(vpi.ValueOf());
      break;
    }
    case ast::Builtin::TableIterClose: {
      GetEmitter()->Emit(Bytecode::TableVectorIteratorFree, iter);
      break;
    }
    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinTableIterParallelCall(ast::CallExpression *call) {
  // First is the table name as a string literal
  const auto table_name = call->GetArguments()[0]->As<ast::LiteralExpression>()->StringVal();
  sql::Table *table = sql::Catalog::Instance()->LookupTableByName(table_name);
  TPL_ASSERT(table != nullptr, "Table does not exist!");

  // Next is the execution context
  LocalVar exec_ctx = VisitExpressionForRValue(call->GetArguments()[1]);

  // Next is the thread state container
  LocalVar thread_state_container = VisitExpressionForRValue(call->GetArguments()[2]);

  // Finally the scan function as an identifier
  const auto scan_fn_name = call->GetArguments()[3]->As<ast::IdentifierExpression>()->GetName();

  // Done
  GetEmitter()->EmitParallelTableScan(table->GetId(), exec_ctx, thread_state_container,
                                      LookupFuncIdByName(scan_fn_name.ToString()));
}

void BytecodeGenerator::VisitBuiltinVPICall(ast::CallExpression *call, ast::Builtin builtin) {
  TPL_ASSERT(call->GetType() != nullptr, "No return type set for call!");

  // The first argument to all calls is a pointer to the VPI
  LocalVar vpi = VisitExpressionForRValue(call->GetArguments()[0]);

  switch (builtin) {
    case ast::Builtin::VPIInit: {
      LocalVar vector_projection = VisitExpressionForRValue(call->GetArguments()[1]);
      if (call->GetArguments().size() == 3) {
        LocalVar tid_list = VisitExpressionForRValue(call->GetArguments()[2]);
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
      LocalVar is_filtered = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::VPIIsFiltered, is_filtered, vpi);
      GetExecutionResult()->SetDestination(is_filtered.ValueOf());
      break;
    }
    case ast::Builtin::VPIGetSelectedRowCount: {
      LocalVar count = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::VPIGetSelectedRowCount, count, vpi);
      GetExecutionResult()->SetDestination(count.ValueOf());
      break;
    }
    case ast::Builtin::VPIGetVectorProjection: {
      LocalVar vector_projection = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::VPIGetVectorProjection, vector_projection, vpi);
      GetExecutionResult()->SetDestination(vector_projection.ValueOf());
      break;
    }
    case ast::Builtin::VPIHasNext: {
      LocalVar cond = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::VPIHasNext, cond, vpi);
      GetExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::VPIAdvance: {
      GetEmitter()->Emit(Bytecode::VPIAdvance, vpi);
      break;
    }
    case ast::Builtin::VPISetPosition: {
      LocalVar index = VisitExpressionForRValue(call->GetArguments()[1]);
      GetEmitter()->Emit(Bytecode::VPISetPosition, vpi, index);
      break;
    }
    case ast::Builtin::VPIMatch: {
      LocalVar match = VisitExpressionForRValue(call->GetArguments()[1]);
      GetEmitter()->Emit(Bytecode::VPIMatch, vpi, match);
      break;
    }
    case ast::Builtin::VPIReset: {
      GetEmitter()->Emit(Bytecode::VPIReset, vpi);
      break;
    }

#define GEN_CASE(BuiltinName, Bytecode)                                                       \
  case ast::Builtin::BuiltinName: {                                                           \
    LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType());          \
    const auto col_idx = call->GetArguments()[1]->As<ast::LiteralExpression>()->IntegerVal(); \
    TPL_ASSERT(col_idx >= 0, "Column index should be non-negative!");                         \
    GetEmitter()->EmitVPIGet(Bytecode, result, vpi, static_cast<uint32_t>(col_idx));          \
    break;                                                                                    \
  }
      // clang-format off
    GEN_CASE(VPIGetBool, Bytecode::VPIGetBool);
    GEN_CASE(VPIGetTinyInt, Bytecode::VPIGetTinyInt);
    GEN_CASE(VPIGetSmallInt, Bytecode::VPIGetSmallInt);
    GEN_CASE(VPIGetInt, Bytecode::VPIGetInteger);
    GEN_CASE(VPIGetBigInt, Bytecode::VPIGetBigInt);
    GEN_CASE(VPIGetReal, Bytecode::VPIGetReal);
    GEN_CASE(VPIGetDouble, Bytecode::VPIGetDouble);
    GEN_CASE(VPIGetDate, Bytecode::VPIGetDate);
    GEN_CASE(VPIGetString, Bytecode::VPIGetString);
    GEN_CASE(VPIGetPointer, Bytecode::VPIGetPointer);
      // clang-format on
#undef GEN_CASE

#define GEN_CASE(BuiltinName, Bytecode)                                                       \
  case ast::Builtin::BuiltinName: {                                                           \
    auto input = VisitExpressionForSQLValue(call->GetArguments()[1]);                         \
    const auto col_idx = call->GetArguments()[1]->As<ast::LiteralExpression>()->IntegerVal(); \
    TPL_ASSERT(col_idx >= 0, "Column index should be non-negative!");                         \
    GetEmitter()->EmitVPISet(Bytecode, vpi, input, static_cast<uint32_t>(col_idx));           \
    break;                                                                                    \
  }

      // clang-format off
    GEN_CASE(VPISetBool, Bytecode::VPISetBool);
    GEN_CASE(VPISetTinyInt, Bytecode::VPISetTinyInt);
    GEN_CASE(VPISetSmallInt, Bytecode::VPISetSmallInt);
    GEN_CASE(VPISetInt, Bytecode::VPISetInteger);
    GEN_CASE(VPISetBigInt, Bytecode::VPISetBigInt);
    GEN_CASE(VPISetReal, Bytecode::VPISetReal);
    GEN_CASE(VPISetDouble, Bytecode::VPISetDouble);
    GEN_CASE(VPISetDate, Bytecode::VPISetDate);
    GEN_CASE(VPISetString, Bytecode::VPISetString);
      // clang-format on
#undef GEN_CASE

    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinCompactStorageCall(ast::CallExpression *call,
                                                       ast::Builtin builtin) {
  LocalVar storage = VisitExpressionForRValue(call->GetArguments()[0]);
  switch (builtin) {
#define GEN_CASE(BuiltinName, Bytecode)                                 \
  case ast::Builtin::BuiltinName: {                                     \
    LocalVar index = VisitExpressionForRValue(call->GetArguments()[1]); \
    LocalVar ptr = VisitExpressionForRValue(call->GetArguments()[2]);   \
    LocalVar val = VisitExpressionForSQLValue(call->GetArguments()[3]); \
    GetEmitter()->Emit(Bytecode, storage, index, ptr, val);             \
    break;                                                              \
  }

    // clang-format off
    GEN_CASE(CompactStorageWriteBool, Bytecode::CompactStorageWriteBool);
    GEN_CASE(CompactStorageWriteTinyInt, Bytecode::CompactStorageWriteTinyInt);
    GEN_CASE(CompactStorageWriteSmallInt, Bytecode::CompactStorageWriteSmallInt);
    GEN_CASE(CompactStorageWriteInteger, Bytecode::CompactStorageWriteInteger);
    GEN_CASE(CompactStorageWriteBigInt, Bytecode::CompactStorageWriteBigInt);
    GEN_CASE(CompactStorageWriteReal, Bytecode::CompactStorageWriteReal);
    GEN_CASE(CompactStorageWriteDouble, Bytecode::CompactStorageWriteDouble);
    GEN_CASE(CompactStorageWriteDate, Bytecode::CompactStorageWriteDate);
    GEN_CASE(CompactStorageWriteTimestamp, Bytecode::CompactStorageWriteTimestamp);
    GEN_CASE(CompactStorageWriteString, Bytecode::CompactStorageWriteString);
    // clang-format on
#undef GEN_CASE

#define GEN_CASE(BuiltinName, Bytecode)                                              \
  case ast::Builtin::BuiltinName: {                                                  \
    LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType()); \
    LocalVar index = VisitExpressionForRValue(call->GetArguments()[1]);              \
    LocalVar ptr = VisitExpressionForRValue(call->GetArguments()[2]);                \
    GetEmitter()->Emit(Bytecode, result, storage, index, ptr);                       \
    break;                                                                           \
  }

    // clang-format off
    GEN_CASE(CompactStorageReadBool, Bytecode::CompactStorageReadBool);
    GEN_CASE(CompactStorageReadTinyInt, Bytecode::CompactStorageReadTinyInt);
    GEN_CASE(CompactStorageReadSmallInt, Bytecode::CompactStorageReadSmallInt);
    GEN_CASE(CompactStorageReadInteger, Bytecode::CompactStorageReadInteger);
    GEN_CASE(CompactStorageReadBigInt, Bytecode::CompactStorageReadBigInt);
    GEN_CASE(CompactStorageReadReal, Bytecode::CompactStorageReadReal);
    GEN_CASE(CompactStorageReadDouble, Bytecode::CompactStorageReadDouble);
    GEN_CASE(CompactStorageReadDate, Bytecode::CompactStorageReadDate);
    GEN_CASE(CompactStorageReadTimestamp, Bytecode::CompactStorageReadTimestamp);
    GEN_CASE(CompactStorageReadString, Bytecode::CompactStorageReadString);
    // clang-format on
#undef GEN_CASE

    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinHashCall(ast::CallExpression *call) {
  TPL_ASSERT(call->GetType()->IsSpecificBuiltin(ast::BuiltinType::UInt64),
             "Return type of @hash(...) expected to be 8-byte unsigned hash");
  TPL_ASSERT(!call->GetArguments().empty(), "@hash() must contain at least one input argument");
  TPL_ASSERT(GetExecutionResult() != nullptr, "Caller of @hash() must use result");

  // The running hash value initialized to zero
  LocalVar hash_val = GetExecutionResult()->GetOrCreateDestination(call->GetType());

  GetEmitter()->EmitAssignImm8(hash_val, 0);

  for (uint32_t idx = 0; idx < call->NumArgs(); idx++) {
    TPL_ASSERT(call->GetArguments()[idx]->GetType()->IsSqlValueType(),
               "Input to hash must be a SQL value type");

    LocalVar input = VisitExpressionForSQLValue(call->GetArguments()[idx]);
    const auto *type = call->GetArguments()[idx]->GetType()->As<ast::BuiltinType>();
    switch (type->GetKind()) {
      case ast::BuiltinType::IntegerVal:
        GetEmitter()->Emit(Bytecode::HashInt, hash_val, input, hash_val.ValueOf());
        break;
      case ast::BuiltinType::RealVal:
        GetEmitter()->Emit(Bytecode::HashReal, hash_val, input, hash_val.ValueOf());
        break;
      case ast::BuiltinType::StringVal:
        GetEmitter()->Emit(Bytecode::HashString, hash_val, input, hash_val.ValueOf());
        break;
      case ast::BuiltinType::DateVal:
        GetEmitter()->Emit(Bytecode::HashDate, hash_val, input, hash_val.ValueOf());
        break;
      case ast::BuiltinType::TimestampVal:
        GetEmitter()->Emit(Bytecode::HashTimestamp, hash_val, input, hash_val.ValueOf());
      default:
        UNREACHABLE("Hashing this type isn't supported!");
    }
  }

  // Set return
  GetExecutionResult()->SetDestination(hash_val.ValueOf());
}

void BytecodeGenerator::VisitBuiltinFilterManagerCall(ast::CallExpression *call,
                                                      ast::Builtin builtin) {
  LocalVar filter_manager = VisitExpressionForRValue(call->GetArguments()[0]);
  switch (builtin) {
    case ast::Builtin::FilterManagerInit: {
      GetEmitter()->Emit(Bytecode::FilterManagerInit, filter_manager);
      break;
    }
    case ast::Builtin::FilterManagerInsertFilter: {
      GetEmitter()->Emit(Bytecode::FilterManagerStartNewClause, filter_manager);

      // Insert all flavors
      for (uint32_t arg_idx = 1; arg_idx < call->NumArgs(); arg_idx++) {
        const std::string func_name =
            call->GetArguments()[arg_idx]->As<ast::IdentifierExpression>()->GetName().ToString();
        const FunctionId func_id = LookupFuncIdByName(func_name);
        GetEmitter()->EmitFilterManagerInsertFilter(filter_manager, func_id);
      }
      break;
    }
    case ast::Builtin::FilterManagerRunFilters: {
      LocalVar vpi = VisitExpressionForRValue(call->GetArguments()[1]);
      GetEmitter()->Emit(Bytecode::FilterManagerRunFilters, filter_manager, vpi);
      break;
    }
    case ast::Builtin::FilterManagerFree: {
      GetEmitter()->Emit(Bytecode::FilterManagerFree, filter_manager);
      break;
    }
    default: {
      UNREACHABLE("Impossible filter manager call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinVectorFilterCall(ast::CallExpression *call,
                                                     ast::Builtin builtin) {
  LocalVar vector_projection = VisitExpressionForRValue(call->GetArguments()[0]);
  LocalVar tid_list = VisitExpressionForRValue(call->GetArguments()[3]);

#define GEN_CASE(BYTECODE)                                                               \
  LocalVar left_col = VisitExpressionForRValue(call->GetArguments()[1]);                 \
  if (!call->GetArguments()[2]->GetType()->IsIntegerType()) {                            \
    LocalVar right_val = VisitExpressionForSQLValue(call->GetArguments()[2]);            \
    GetEmitter()->Emit(BYTECODE##Val, vector_projection, left_col, right_val, tid_list); \
  } else {                                                                               \
    LocalVar right_col = VisitExpressionForRValue(call->GetArguments()[2]);              \
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
    default: {
      UNREACHABLE("Impossible vector filter executor call");
    }
  }
#undef GEN_CASE
}

void BytecodeGenerator::VisitBuiltinAggHashTableCall(ast::CallExpression *call,
                                                     ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggHashTableInit: {
      LocalVar agg_ht = VisitExpressionForRValue(call->GetArguments()[0]);
      LocalVar memory = VisitExpressionForRValue(call->GetArguments()[1]);
      LocalVar entry_size = VisitExpressionForRValue(call->GetArguments()[2]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableInit, agg_ht, memory, entry_size);
      break;
    }
    case ast::Builtin::AggHashTableInsert: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg_ht = VisitExpressionForRValue(call->GetArguments()[0]);
      LocalVar hash = VisitExpressionForRValue(call->GetArguments()[1]);
      Bytecode bytecode = Bytecode::AggregationHashTableAllocTuple;
      if (call->GetArguments().size() > 2) {
        TPL_ASSERT(call->GetArguments()[2]->IsBoolLiteral(),
                   "Last argument must be a boolean literal");
        const bool partitioned = call->GetArguments()[2]->As<ast::LiteralExpression>()->BoolVal();
        bytecode = partitioned ? Bytecode::AggregationHashTableAllocTuplePartitioned
                               : Bytecode::AggregationHashTableAllocTuple;
      }
      GetEmitter()->Emit(bytecode, dest, agg_ht, hash);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableLinkEntry: {
      LocalVar agg_ht = VisitExpressionForRValue(call->GetArguments()[0]);
      LocalVar entry = VisitExpressionForRValue(call->GetArguments()[1]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableLinkHashTableEntry, agg_ht, entry);
      break;
    }
    case ast::Builtin::AggHashTableLookup: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg_ht = VisitExpressionForRValue(call->GetArguments()[0]);
      LocalVar hash = VisitExpressionForRValue(call->GetArguments()[1]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableLookup, dest, agg_ht, hash);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableProcessBatch: {
      LocalVar agg_ht = VisitExpressionForRValue(call->GetArguments()[0]);
      LocalVar vpi = VisitExpressionForRValue(call->GetArguments()[1]);
      uint32_t num_keys = call->GetArguments()[2]->GetType()->As<ast::ArrayType>()->GetLength();
      LocalVar key_cols = VisitExpressionForLValue(call->GetArguments()[2]);
      auto init_agg_fn = LookupFuncIdByName(
          call->GetArguments()[3]->As<ast::IdentifierExpression>()->GetName().ToString());
      auto merge_agg_fn = LookupFuncIdByName(
          call->GetArguments()[4]->As<ast::IdentifierExpression>()->GetName().ToString());
      LocalVar partitioned = VisitExpressionForRValue(call->GetArguments()[5]);
      GetEmitter()->EmitAggHashTableProcessBatch(agg_ht, vpi, num_keys, key_cols, init_agg_fn,
                                                 merge_agg_fn, partitioned);
      break;
    }
    case ast::Builtin::AggHashTableMovePartitions: {
      LocalVar agg_ht = VisitExpressionForRValue(call->GetArguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->GetArguments()[1]);
      LocalVar aht_offset = VisitExpressionForRValue(call->GetArguments()[2]);
      auto merge_part_fn = LookupFuncIdByName(
          call->GetArguments()[3]->As<ast::IdentifierExpression>()->GetName().ToString());
      GetEmitter()->EmitAggHashTableMovePartitions(agg_ht, tls, aht_offset, merge_part_fn);
      break;
    }
    case ast::Builtin::AggHashTableParallelPartitionedScan: {
      LocalVar agg_ht = VisitExpressionForRValue(call->GetArguments()[0]);
      LocalVar ctx = VisitExpressionForRValue(call->GetArguments()[1]);
      LocalVar tls = VisitExpressionForRValue(call->GetArguments()[2]);
      auto scan_part_fn = LookupFuncIdByName(
          call->GetArguments()[3]->As<ast::IdentifierExpression>()->GetName().ToString());
      GetEmitter()->EmitAggHashTableParallelPartitionedScan(agg_ht, ctx, tls, scan_part_fn);
      break;
    }
    case ast::Builtin::AggHashTableFree: {
      LocalVar agg_ht = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableFree, agg_ht);
      break;
    }
    default: {
      UNREACHABLE("Impossible aggregation hash table bytecode");
    }
  }
}

void BytecodeGenerator::VisitBuiltinAggHashTableIterCall(ast::CallExpression *call,
                                                         ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggHashTableIterInit: {
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->GetArguments()[0]);
      LocalVar agg_ht = VisitExpressionForRValue(call->GetArguments()[1]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableIteratorInit, agg_ht_iter, agg_ht);
      break;
    }
    case ast::Builtin::AggHashTableIterHasNext: {
      LocalVar has_more = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableIteratorHasNext, has_more, agg_ht_iter);
      GetExecutionResult()->SetDestination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableIterNext: {
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableIteratorNext, agg_ht_iter);
      break;
    }
    case ast::Builtin::AggHashTableIterGetRow: {
      LocalVar row_ptr = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableIteratorGetRow, row_ptr, agg_ht_iter);
      GetExecutionResult()->SetDestination(row_ptr.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableIterClose: {
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableIteratorFree, agg_ht_iter);
      break;
    }
    default: {
      UNREACHABLE("Impossible aggregation hash table iteration bytecode");
    }
  }
}

void BytecodeGenerator::VisitBuiltinAggPartIterCall(ast::CallExpression *call,
                                                    ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggPartIterHasNext: {
      LocalVar has_more = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar iter = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorHasNext, has_more, iter);
      GetExecutionResult()->SetDestination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::AggPartIterNext: {
      LocalVar iter = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorNext, iter);
      break;
    }
    case ast::Builtin::AggPartIterGetRow: {
      LocalVar row = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar iter = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorGetRow, row, iter);
      GetExecutionResult()->SetDestination(row.ValueOf());
      break;
    }
    case ast::Builtin::AggPartIterGetRowEntry: {
      LocalVar entry = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar iter = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorGetRowEntry, entry, iter);
      GetExecutionResult()->SetDestination(entry.ValueOf());
      break;
    }
    case ast::Builtin::AggPartIterGetHash: {
      LocalVar hash = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar iter = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorGetHash, hash, iter);
      GetExecutionResult()->SetDestination(hash.ValueOf());
      break;
    }
    default: {
      UNREACHABLE("Impossible aggregation partition iterator bytecode");
    }
  }
}

namespace {

// All aggregate types and bytecodes. Aggregates implement a common interface. Thus, their bytecode
// can be generated by only knowing the type of aggregate.
// Format: Type, Init Op, Advance Op, Result Op, Merge Op, Reset Op, Free Op
#define AGG_CODES(F)                                                                            \
  /* COUNT(col) */                                                                              \
  F(CountAggregate, CountAggregateInit, CountAggregateAdvance, CountAggregateGetResult,         \
    CountAggregateMerge, CountAggregateReset, CountAggregateFree)                               \
  /* COUNT(*) */                                                                                \
  F(CountStarAggregate, CountStarAggregateInit, CountStarAggregateAdvance,                      \
    CountStarAggregateGetResult, CountStarAggregateMerge, CountStarAggregateReset,              \
    CountStarAggregateFree)                                                                     \
  /* AVG(col) */                                                                                \
  F(AvgAggregate, AvgAggregateInit, AvgAggregateAdvanceInteger, AvgAggregateGetResult,          \
    AvgAggregateMerge, AvgAggregateReset, AvgAggregateFree)                                     \
  /* MAX(int_col) */                                                                            \
  F(IntegerMaxAggregate, IntegerMaxAggregateInit, IntegerMaxAggregateAdvance,                   \
    IntegerMaxAggregateGetResult, IntegerMaxAggregateMerge, IntegerMaxAggregateReset,           \
    IntegerMaxAggregateFree)                                                                    \
  /* MIN(int_col) */                                                                            \
  F(IntegerMinAggregate, IntegerMinAggregateInit, IntegerMinAggregateAdvance,                   \
    IntegerMinAggregateGetResult, IntegerMinAggregateMerge, IntegerMinAggregateReset,           \
    IntegerMinAggregateFree)                                                                    \
  /* SUM(int_col) */                                                                            \
  F(IntegerSumAggregate, IntegerSumAggregateInit, IntegerSumAggregateAdvance,                   \
    IntegerSumAggregateGetResult, IntegerSumAggregateMerge, IntegerSumAggregateReset,           \
    IntegerSumAggregateFree)                                                                    \
  /* MAX(real_col) */                                                                           \
  F(RealMaxAggregate, RealMaxAggregateInit, RealMaxAggregateAdvance, RealMaxAggregateGetResult, \
    RealMaxAggregateMerge, RealMaxAggregateReset, RealMaxAggregateFree)                         \
  /* MIN(real_col) */                                                                           \
  F(RealMinAggregate, RealMinAggregateInit, RealMinAggregateAdvance, RealMinAggregateGetResult, \
    RealMinAggregateMerge, RealMinAggregateReset, RealMinAggregateFree)                         \
  /* SUM(real_col) */                                                                           \
  F(RealSumAggregate, RealSumAggregateInit, RealSumAggregateAdvance, RealSumAggregateGetResult, \
    RealSumAggregateMerge, RealSumAggregateReset, RealSumAggregateFree)                         \
  /* MAX(date_col) */                                                                           \
  F(DateMaxAggregate, DateMaxAggregateInit, DateMaxAggregateAdvance, DateMaxAggregateGetResult, \
    DateMaxAggregateMerge, DateMaxAggregateReset, DateMaxAggregateFree)                         \
  /* MIN(date_col) */                                                                           \
  F(DateMinAggregate, DateMinAggregateInit, DateMinAggregateAdvance, DateMinAggregateGetResult, \
    DateMinAggregateMerge, DateMinAggregateReset, DateMinAggregateFree)                         \
  /* MAX(string_col) */                                                                         \
  F(StringMaxAggregate, StringMaxAggregateInit, StringMaxAggregateAdvance,                      \
    StringMaxAggregateGetResult, StringMaxAggregateMerge, StringMaxAggregateReset,              \
    StringMaxAggregateFree)                                                                     \
  /* MIN(string_col) */                                                                         \
  F(StringMinAggregate, StringMinAggregateInit, StringMinAggregateAdvance,                      \
    StringMinAggregateGetResult, StringMinAggregateMerge, StringMinAggregateReset,              \
    StringMinAggregateFree)

enum class AggOpKind : uint8_t {
  Init = 0,
  Advance = 1,
  GetResult = 2,
  Merge = 3,
  Reset = 4,
  Free = 5
};

// Given an aggregate kind and the operation to perform on it, determine the
// appropriate bytecode
template <AggOpKind OpKind>
Bytecode OpForAgg(ast::BuiltinType::Kind agg_kind);

template <>
Bytecode OpForAgg<AggOpKind::Init>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: {
      UNREACHABLE("Impossible aggregate type");
    }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset, Free) \
  case ast::BuiltinType::Type:                                    \
    return Bytecode::Init;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

template <>
Bytecode OpForAgg<AggOpKind::Advance>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: {
      UNREACHABLE("Impossible aggregate type");
    }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset, Free) \
  case ast::BuiltinType::Type:                                    \
    return Bytecode::Advance;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

template <>
Bytecode OpForAgg<AggOpKind::GetResult>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: {
      UNREACHABLE("Impossible aggregate type");
    }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset, Free) \
  case ast::BuiltinType::Type:                                    \
    return Bytecode::GetResult;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

template <>
Bytecode OpForAgg<AggOpKind::Merge>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: {
      UNREACHABLE("Impossible aggregate type");
    }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset, Free) \
  case ast::BuiltinType::Type:                                    \
    return Bytecode::Merge;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

template <>
Bytecode OpForAgg<AggOpKind::Reset>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: {
      UNREACHABLE("Impossible aggregate type");
    }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset, Free) \
  case ast::BuiltinType::Type:                                    \
    return Bytecode::Reset;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

}  // namespace

void BytecodeGenerator::VisitBuiltinAggregatorCall(ast::CallExpression *call,
                                                   ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggInit:
    case ast::Builtin::AggReset: {
      for (const auto &arg : call->GetArguments()) {
        const auto agg_kind = arg->GetType()->GetPointeeType()->As<ast::BuiltinType>()->GetKind();
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
      const auto &args = call->GetArguments();
      const auto agg_kind = args[0]->GetType()->GetPointeeType()->As<ast::BuiltinType>()->GetKind();
      LocalVar agg = VisitExpressionForRValue(args[0]);
      LocalVar input = VisitExpressionForSQLValue(args[1]);
      Bytecode bytecode = OpForAgg<AggOpKind::Advance>(agg_kind);

      // Hack to handle advancing AvgAggregates with float/double precision numbers. The default
      // behavior in OpForAgg() is to use AvgAggregateAdvanceInteger.
      if (agg_kind == ast::BuiltinType::AvgAggregate &&
          args[1]->GetType()->IsSpecificBuiltin(ast::BuiltinType::RealVal)) {
        bytecode = Bytecode::AvgAggregateAdvanceReal;
      }

      GetEmitter()->Emit(bytecode, agg, input);
      break;
    }
    case ast::Builtin::AggMerge: {
      const auto &args = call->GetArguments();
      const auto agg_kind = args[0]->GetType()->GetPointeeType()->As<ast::BuiltinType>()->GetKind();
      LocalVar agg_1 = VisitExpressionForRValue(args[0]);
      LocalVar agg_2 = VisitExpressionForRValue(args[1]);
      Bytecode bytecode = OpForAgg<AggOpKind::Merge>(agg_kind);
      GetEmitter()->Emit(bytecode, agg_1, agg_2);
      break;
    }
    case ast::Builtin::AggResult: {
      const auto &args = call->GetArguments();
      const auto agg_kind = args[0]->GetType()->GetPointeeType()->As<ast::BuiltinType>()->GetKind();
      LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg = VisitExpressionForRValue(args[0]);
      Bytecode bytecode = OpForAgg<AggOpKind::GetResult>(agg_kind);
      GetEmitter()->Emit(bytecode, result, agg);
      break;
    }
    default: {
      UNREACHABLE("Impossible aggregator call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinJoinHashTableCall(ast::CallExpression *call,
                                                      ast::Builtin builtin) {
  // The join hash table is always the first argument to all JHT calls
  LocalVar join_hash_table = VisitExpressionForRValue(call->GetArguments()[0]);

  switch (builtin) {
    case ast::Builtin::JoinHashTableInit: {
      LocalVar memory = VisitExpressionForRValue(call->GetArguments()[1]);
      LocalVar entry_size = VisitExpressionForRValue(call->GetArguments()[2]);
      if (call->NumArgs() == 3) {
        GetEmitter()->Emit(Bytecode::JoinHashTableInit, join_hash_table, memory, entry_size);
      } else {
        auto analysis_fn = call->GetArguments()[3]->As<ast::IdentifierExpression>()->GetName();
        auto compress_fn = call->GetArguments()[3]->As<ast::IdentifierExpression>()->GetName();
        GetEmitter()->EmitJoinHashTableInit(join_hash_table, memory, entry_size,
                                            LookupFuncIdByName(analysis_fn.ToString()),
                                            LookupFuncIdByName(compress_fn.ToString()));
      }
      break;
    }
    case ast::Builtin::JoinHashTableInsert: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar hash = VisitExpressionForRValue(call->GetArguments()[1]);
      GetEmitter()->Emit(Bytecode::JoinHashTableAllocTuple, dest, join_hash_table, hash);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::JoinHashTableBuild: {
      GetEmitter()->Emit(Bytecode::JoinHashTableBuild, join_hash_table);
      break;
    }
    case ast::Builtin::JoinHashTableBuildParallel: {
      LocalVar tls = VisitExpressionForRValue(call->GetArguments()[1]);
      LocalVar jht_offset = VisitExpressionForRValue(call->GetArguments()[2]);
      GetEmitter()->Emit(Bytecode::JoinHashTableBuildParallel, join_hash_table, tls, jht_offset);
      break;
    }
    case ast::Builtin::JoinHashTableLookup: {
      LocalVar ht_entry = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar hash = VisitExpressionForRValue(call->GetArguments()[1]);
      GetEmitter()->Emit(Bytecode::JoinHashTableLookup, ht_entry, join_hash_table, hash);
      GetExecutionResult()->SetDestination(ht_entry.ValueOf());
      break;
    }
    case ast::Builtin::JoinHashTableFree: {
      GetEmitter()->Emit(Bytecode::JoinHashTableFree, join_hash_table);
      break;
    }
    default: {
      UNREACHABLE("Impossible join hash table call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinHashTableEntryCall(ast::CallExpression *call,
                                                       ast::Builtin builtin) {
  // The hash table entry iterator is always the first argument to all calls
  LocalVar ht_entry = VisitExpressionForRValue(call->GetArguments()[0]);

  switch (builtin) {
    case ast::Builtin::HashTableEntryGetHash: {
      LocalVar hash = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::HashTableEntryGetHash, hash, ht_entry);
      GetExecutionResult()->SetDestination(hash.ValueOf());
      break;
    }
    case ast::Builtin::HashTableEntryGetRow: {
      LocalVar row = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::HashTableEntryGetRow, row, ht_entry);
      GetExecutionResult()->SetDestination(row.ValueOf());
      break;
    }
    case ast::Builtin::HashTableEntryGetNext: {
      LocalVar next_entry = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::HashTableEntryGetNext, next_entry, ht_entry);
      GetExecutionResult()->SetDestination(next_entry.ValueOf());
      break;
    }
    default: {
      UNREACHABLE("Impossible hash table entry iterator call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinAnalysisStatsCall(ast::CallExpression *call,
                                                      ast::Builtin builtin) {
  LocalVar stats = VisitExpressionForRValue(call->GetArguments()[0]);
  switch (builtin) {
    case ast::Builtin::AnalysisStatsSetColumnCount: {
      LocalVar column_count = VisitExpressionForRValue(call->GetArguments()[1]);
      GetEmitter()->Emit(Bytecode::AnalysisStatsSetColumnCount, stats, column_count);
      break;
    }
    case ast::Builtin::AnalysisStatsSetColumnBits: {
      LocalVar column = VisitExpressionForRValue(call->GetArguments()[1]);
      LocalVar bits = VisitExpressionForRValue(call->GetArguments()[2]);
      GetEmitter()->Emit(Bytecode::AnalysisStatsSetColumnBits, stats, column, bits);
      break;
    }
    default: {
      UNREACHABLE("Impossible @stats() call.");
    }
  }
}

void BytecodeGenerator::VisitBuiltinSorterCall(ast::CallExpression *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::SorterInit: {
      // TODO(pmenon): Fix me so that the comparison function doesn't have be
      // listed by name.
      LocalVar sorter = VisitExpressionForRValue(call->GetArguments()[0]);
      LocalVar memory = VisitExpressionForRValue(call->GetArguments()[1]);
      const std::string cmp_func_name =
          call->GetArguments()[2]->As<ast::IdentifierExpression>()->GetName().ToString();
      LocalVar entry_size = VisitExpressionForRValue(call->GetArguments()[3]);
      GetEmitter()->EmitSorterInit(Bytecode::SorterInit, sorter, memory,
                                   LookupFuncIdByName(cmp_func_name), entry_size);
      break;
    }
    case ast::Builtin::SorterInsert: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar sorter = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::SorterAllocTuple, dest, sorter);
      break;
    }
    case ast::Builtin::SorterInsertTopK: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar sorter = VisitExpressionForRValue(call->GetArguments()[0]);
      LocalVar top_k = VisitExpressionForRValue(call->GetArguments()[1]);
      GetEmitter()->Emit(Bytecode::SorterAllocTupleTopK, dest, sorter, top_k);
      break;
    }
    case ast::Builtin::SorterInsertTopKFinish: {
      LocalVar sorter = VisitExpressionForRValue(call->GetArguments()[0]);
      LocalVar top_k = VisitExpressionForRValue(call->GetArguments()[1]);
      GetEmitter()->Emit(Bytecode::SorterAllocTupleTopKFinish, sorter, top_k);
      break;
    }
    case ast::Builtin::SorterSort: {
      LocalVar sorter = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::SorterSort, sorter);
      break;
    }
    case ast::Builtin::SorterSortParallel: {
      LocalVar sorter = VisitExpressionForRValue(call->GetArguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->GetArguments()[1]);
      LocalVar sorter_offset = VisitExpressionForRValue(call->GetArguments()[2]);
      GetEmitter()->Emit(Bytecode::SorterSortParallel, sorter, tls, sorter_offset);
      break;
    }
    case ast::Builtin::SorterSortTopKParallel: {
      LocalVar sorter = VisitExpressionForRValue(call->GetArguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->GetArguments()[1]);
      LocalVar sorter_offset = VisitExpressionForRValue(call->GetArguments()[2]);
      LocalVar top_k = VisitExpressionForRValue(call->GetArguments()[3]);
      GetEmitter()->Emit(Bytecode::SorterSortTopKParallel, sorter, tls, sorter_offset, top_k);
      break;
    }
    case ast::Builtin::SorterFree: {
      LocalVar sorter = VisitExpressionForRValue(call->GetArguments()[0]);
      GetEmitter()->Emit(Bytecode::SorterFree, sorter);
      break;
    }
    default: {
      UNREACHABLE("Impossible bytecode");
    }
  }
}

void BytecodeGenerator::VisitBuiltinSorterIterCall(ast::CallExpression *call,
                                                   ast::Builtin builtin) {
  // The first argument to all calls is the sorter iterator instance
  const LocalVar sorter_iter = VisitExpressionForRValue(call->GetArguments()[0]);

  switch (builtin) {
    case ast::Builtin::SorterIterInit: {
      LocalVar sorter = VisitExpressionForRValue(call->GetArguments()[1]);
      GetEmitter()->Emit(Bytecode::SorterIteratorInit, sorter_iter, sorter);
      break;
    }
    case ast::Builtin::SorterIterHasNext: {
      LocalVar cond = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::SorterIteratorHasNext, cond, sorter_iter);
      GetExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::SorterIterNext: {
      GetEmitter()->Emit(Bytecode::SorterIteratorNext, sorter_iter);
      break;
    }
    case ast::Builtin::SorterIterSkipRows: {
      LocalVar n = VisitExpressionForRValue(call->GetArguments()[1]);
      GetEmitter()->Emit(Bytecode::SorterIteratorSkipRows, sorter_iter, n);
      break;
    }
    case ast::Builtin::SorterIterGetRow: {
      LocalVar row_ptr = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::SorterIteratorGetRow, row_ptr, sorter_iter);
      GetExecutionResult()->SetDestination(row_ptr.ValueOf());
      break;
    }
    case ast::Builtin::SorterIterClose: {
      GetEmitter()->Emit(Bytecode::SorterIteratorFree, sorter_iter);
      break;
    }
    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void BytecodeGenerator::VisitResultBufferCall(ast::CallExpression *call, ast::Builtin builtin) {
  LocalVar exec_ctx = VisitExpressionForRValue(call->GetArguments()[0]);
  switch (builtin) {
    case ast::Builtin::ResultBufferAllocOutRow: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::ResultBufferAllocOutputRow, dest, exec_ctx);
      break;
    }
    case ast::Builtin::ResultBufferFinalize: {
      GetEmitter()->Emit(Bytecode::ResultBufferFinalize, exec_ctx);
      break;
    }
    default: {
      UNREACHABLE("Invalid result buffer call!");
    }
  }
}

void BytecodeGenerator::VisitCSVReaderCall(ast::CallExpression *call, ast::Builtin builtin) {
  LocalVar reader = VisitExpressionForRValue(call->GetArguments()[0]);
  switch (builtin) {
    case ast::Builtin::CSVReaderInit: {
      LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      TPL_ASSERT(call->GetArguments()[1]->IsLiteralExpression(),
                 "Second argument expected to be string literal");
      auto string_lit = call->GetArguments()[1]->As<ast::LiteralExpression>()->StringVal();
      auto file_name = NewStaticString(call->GetType()->GetContext(), string_lit);
      GetEmitter()->EmitCSVReaderInit(reader, file_name, string_lit.GetLength());
      GetEmitter()->Emit(Bytecode::CSVReaderPerformInit, result, reader);
      GetExecutionResult()->SetDestination(result.ValueOf());
      break;
    }
    case ast::Builtin::CSVReaderAdvance: {
      LocalVar has_more = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::CSVReaderAdvance, has_more, reader);
      GetExecutionResult()->SetDestination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::CSVReaderGetField: {
      LocalVar field_index = VisitExpressionForRValue(call->GetArguments()[1]);
      LocalVar field = VisitExpressionForRValue(call->GetArguments()[2]);
      GetEmitter()->Emit(Bytecode::CSVReaderGetField, reader, field_index, field);
      break;
    }
    case ast::Builtin::CSVReaderGetRecordNumber: {
      LocalVar record_number = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::CSVReaderGetRecordNumber, record_number, reader);
      break;
    }
    case ast::Builtin::CSVReaderClose: {
      GetEmitter()->Emit(Bytecode::CSVReaderClose, reader);
      break;
    }
    default: {
      UNREACHABLE("Invalid CSV reader call!");
    }
  }
}

void BytecodeGenerator::VisitExecutionContextCall(ast::CallExpression *call, ast::Builtin builtin) {
  LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  LocalVar exec_ctx = VisitExpressionForRValue(call->GetArguments()[0]);
  switch (builtin) {
    case ast::Builtin::ExecutionContextGetMemoryPool: {
      GetEmitter()->Emit(Bytecode::ExecutionContextGetMemoryPool, result, exec_ctx);
      break;
    }
    case ast::Builtin::ExecutionContextGetTLS: {
      GetEmitter()->Emit(Bytecode::ExecutionContextGetTLS, result, exec_ctx);
      break;
    }
    default: {
      UNREACHABLE("Impossible execution context call");
    }
  }
  GetExecutionResult()->SetDestination(result.ValueOf());
}

void BytecodeGenerator::VisitBuiltinThreadStateContainerCall(ast::CallExpression *call,
                                                             ast::Builtin builtin) {
  LocalVar tls = VisitExpressionForRValue(call->GetArguments()[0]);
  switch (builtin) {
    case ast::Builtin::ThreadStateContainerGetState: {
      LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::ThreadStateContainerAccessCurrentThreadState, result, tls);
      GetExecutionResult()->SetDestination(result.ValueOf());
      break;
    }
    case ast::Builtin::ThreadStateContainerIterate: {
      LocalVar ctx = VisitExpressionForRValue(call->GetArguments()[1]);
      FunctionId iterate_fn = LookupFuncIdByName(
          call->GetArguments()[2]->As<ast::IdentifierExpression>()->GetName().ToString());
      GetEmitter()->EmitThreadStateContainerIterate(tls, ctx, iterate_fn);
      break;
    }
    case ast::Builtin::ThreadStateContainerReset: {
      LocalVar entry_size = VisitExpressionForRValue(call->GetArguments()[1]);
      FunctionId init_fn = LookupFuncIdByName(
          call->GetArguments()[2]->As<ast::IdentifierExpression>()->GetName().ToString());
      FunctionId destroy_fn = LookupFuncIdByName(
          call->GetArguments()[3]->As<ast::IdentifierExpression>()->GetName().ToString());
      LocalVar ctx = VisitExpressionForRValue(call->GetArguments()[4]);
      GetEmitter()->EmitThreadStateContainerReset(tls, entry_size, init_fn, destroy_fn, ctx);
      break;
    }
    case ast::Builtin::ThreadStateContainerClear: {
      GetEmitter()->Emit(Bytecode::ThreadStateContainerClear, tls);
      break;
    }
    default: {
      UNREACHABLE("Impossible thread state container call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinTrigCall(ast::CallExpression *call, ast::Builtin builtin) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  LocalVar src = VisitExpressionForSQLValue(call->GetArguments()[0]);

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
      LocalVar src2 = VisitExpressionForSQLValue(call->GetArguments()[1]);
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
    default: {
      UNREACHABLE("Impossible trigonometric bytecode");
    }
  }
}

void BytecodeGenerator::VisitBuiltinBitsCall(ast::CallExpression *call, ast::Builtin builtin) {
  const bool caller_wants_result = GetExecutionResult() != nullptr;

  LocalVar result;
  if (caller_wants_result) {
    result = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  } else {
    result = GetCurrentFunction()->NewLocal(call->GetType());
  }

  LocalVar input = VisitExpressionForRValue(call->GetArguments()[0]);

  Bytecode op;
  switch (ast::Type *input_type = call->GetArguments()[0]->GetType(); builtin) {
    case ast::Builtin::Ctlz: {
      op = GetIntTypedBytecode(GET_BASE_FOR_UINT_TYPES(Bytecode::BitCtlz), input_type, false);
      break;
    }
    case ast::Builtin::Cttz: {
      op = GetIntTypedBytecode(GET_BASE_FOR_UINT_TYPES(Bytecode::BitCttz), input_type, false);
      break;
    }
    default: {
      UNREACHABLE("Impossible bit-based bytecode");
    }
  }

  GetEmitter()->Emit(op, result, input);

  if (caller_wants_result) {
    GetExecutionResult()->SetDestination(result.ValueOf());
  }
}

void BytecodeGenerator::VisitBuiltinSizeOfCall(ast::CallExpression *call) {
  ast::Type *target_type = call->GetArguments()[0]->GetType();
  LocalVar size_var = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  GetEmitter()->EmitAssignImm4(size_var, target_type->GetSize());
  GetExecutionResult()->SetDestination(size_var.ValueOf());
}

void BytecodeGenerator::VisitBuiltinOffsetOfCall(ast::CallExpression *call) {
  auto composite_type = call->GetArguments()[0]->GetType()->As<ast::StructType>();
  auto field_name = call->GetArguments()[1]->As<ast::IdentifierExpression>();
  const uint32_t offset = composite_type->GetOffsetOfFieldByName(field_name->GetName());
  LocalVar offset_var = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  GetEmitter()->EmitAssignImm4(offset_var, offset);
  GetExecutionResult()->SetDestination(offset_var.ValueOf());
}

void BytecodeGenerator::VisitBuiltinIntCastCall(ast::CallExpression *call) {
  // Does the caller want the result of the cast? Most likely, yes.
  const bool caller_wants_result = GetExecutionResult() != nullptr;

  LocalVar dest;
  if (caller_wants_result) {
    dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  } else {
    dest = GetCurrentFunction()->NewLocal(call->GetType());
  }
  LocalVar input = VisitExpressionForRValue(call->GetArguments()[1]);

  // Cast input->target.
  const auto input_type = call->GetArguments()[1]->GetType()->As<ast::BuiltinType>()->GetKind();
  const auto target_type = call->GetType()->As<ast::BuiltinType>()->GetKind();

  // clang-format off
#define EMIT_CAST(type1, type2) GetEmitter()->Emit(Bytecode::Cast_##type1##_##type2, dest, input);

#define DISPATCH(type)                                                         \
  switch (target_type) {                                                       \
    case ast::BuiltinType::Bool: EMIT_CAST(type, bool); break;                 \
    case ast::BuiltinType::Int8: EMIT_CAST(type, int8_t); break;               \
    case ast::BuiltinType::Int16:EMIT_CAST(type, int16_t); break;              \
    case ast::BuiltinType::Int32: EMIT_CAST(type, int32_t); break;             \
    case ast::BuiltinType::Int64: EMIT_CAST(type, int64_t); break;             \
    case ast::BuiltinType::UInt8: EMIT_CAST(type, uint8_t); break;             \
    case ast::BuiltinType::UInt16: EMIT_CAST(type, uint16_t); break;           \
    case ast::BuiltinType::UInt32: EMIT_CAST(type, uint32_t); break;           \
    case ast::BuiltinType::UInt64: EMIT_CAST(type, uint64_t); break;           \
    default: UNREACHABLE("Impossible integer type.");                          \
  }

  switch (input_type) {
    case ast::BuiltinType::Bool: DISPATCH(bool); break;
    case ast::BuiltinType::Int8: DISPATCH(int8_t); break;
    case ast::BuiltinType::Int16: DISPATCH(int16_t); break;
    case ast::BuiltinType::Int32: DISPATCH(int32_t); break;
    case ast::BuiltinType::Int64: DISPATCH(int64_t); break;
    case ast::BuiltinType::UInt8: DISPATCH(uint8_t); break;
    case ast::BuiltinType::UInt16: DISPATCH(uint16_t); break;
    case ast::BuiltinType::UInt32: DISPATCH(uint32_t); break;
    case ast::BuiltinType::UInt64: DISPATCH(uint64_t); break;
    default: UNREACHABLE("Impossible integer type.");
  }
    // clang-format on

#undef DISPATCH
#undef EMIT

  if (caller_wants_result) {
    GetExecutionResult()->SetDestination(dest.ValueOf());
  }
}

void BytecodeGenerator::VisitBuiltinCallExpression(ast::CallExpression *call) {
  ast::Builtin builtin;

  ast::Context *ctx = call->GetType()->GetContext();
  ctx->IsBuiltinFunction(call->GetFuncName(), &builtin);

  switch (builtin) {
    case ast::Builtin::BoolToSql:
    case ast::Builtin::IntToSql:
    case ast::Builtin::FloatToSql:
    case ast::Builtin::DateToSql:
    case ast::Builtin::StringToSql:
    case ast::Builtin::SqlToBool:
    case ast::Builtin::ConvertBoolToInteger:
    case ast::Builtin::ConvertIntegerToReal:
    case ast::Builtin::ConvertDateToTimestamp:
    case ast::Builtin::ConvertStringToBool:
    case ast::Builtin::ConvertStringToInt:
    case ast::Builtin::ConvertStringToReal:
    case ast::Builtin::ConvertStringToDate:
    case ast::Builtin::ConvertStringToTime: {
      VisitSqlConversionCall(call, builtin);
      break;
    }
    case ast::Builtin::IsValNull:
    case ast::Builtin::InitSqlNull: {
      VisitNullValueCall(call, builtin);
      break;
    }
    case ast::Builtin::LNot: {
      VisitLNotCall(call);
      break;
    }
    case ast::Builtin::Like: {
      VisitSqlStringLikeCall(call);
      break;
    }
    case ast::Builtin::ExtractYear: {
      VisitBuiltinDateFunctionCall(call, builtin);
      break;
    }
    case ast::Builtin::Concat: {
      VisitBuiltinConcatCall(call);
      break;
    }
    case ast::Builtin::ExecutionContextGetMemoryPool:
    case ast::Builtin::ExecutionContextGetTLS: {
      VisitExecutionContextCall(call, builtin);
      break;
    }
    case ast::Builtin::ThreadStateContainerIterate:
    case ast::Builtin::ThreadStateContainerGetState:
    case ast::Builtin::ThreadStateContainerReset:
    case ast::Builtin::ThreadStateContainerClear: {
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
    case ast::Builtin::VPIAdvance:
    case ast::Builtin::VPISetPosition:
    case ast::Builtin::VPIMatch:
    case ast::Builtin::VPIReset:
    case ast::Builtin::VPIGetBool:
    case ast::Builtin::VPIGetTinyInt:
    case ast::Builtin::VPIGetSmallInt:
    case ast::Builtin::VPIGetInt:
    case ast::Builtin::VPIGetBigInt:
    case ast::Builtin::VPIGetReal:
    case ast::Builtin::VPIGetDouble:
    case ast::Builtin::VPIGetDate:
    case ast::Builtin::VPIGetString:
    case ast::Builtin::VPIGetPointer:
    case ast::Builtin::VPISetBool:
    case ast::Builtin::VPISetTinyInt:
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
    case ast::Builtin::CompactStorageWriteBool:
    case ast::Builtin::CompactStorageWriteTinyInt:
    case ast::Builtin::CompactStorageWriteSmallInt:
    case ast::Builtin::CompactStorageWriteInteger:
    case ast::Builtin::CompactStorageWriteBigInt:
    case ast::Builtin::CompactStorageWriteReal:
    case ast::Builtin::CompactStorageWriteDouble:
    case ast::Builtin::CompactStorageWriteDate:
    case ast::Builtin::CompactStorageWriteTimestamp:
    case ast::Builtin::CompactStorageWriteString:
    case ast::Builtin::CompactStorageReadBool:
    case ast::Builtin::CompactStorageReadTinyInt:
    case ast::Builtin::CompactStorageReadSmallInt:
    case ast::Builtin::CompactStorageReadInteger:
    case ast::Builtin::CompactStorageReadBigInt:
    case ast::Builtin::CompactStorageReadReal:
    case ast::Builtin::CompactStorageReadDouble:
    case ast::Builtin::CompactStorageReadDate:
    case ast::Builtin::CompactStorageReadTimestamp:
    case ast::Builtin::CompactStorageReadString: {
      VisitBuiltinCompactStorageCall(call, builtin);
      break;
    }
    case ast::Builtin::Hash: {
      VisitBuiltinHashCall(call);
      break;
    };
    case ast::Builtin::FilterManagerInit:
    case ast::Builtin::FilterManagerInsertFilter:
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
    case ast::Builtin::HashTableEntryGetHash:
    case ast::Builtin::HashTableEntryGetRow:
    case ast::Builtin::HashTableEntryGetNext: {
      VisitBuiltinHashTableEntryCall(call, builtin);
      break;
    }
    case ast::Builtin::AnalysisStatsSetColumnCount:
    case ast::Builtin::AnalysisStatsSetColumnBits: {
      VisitBuiltinAnalysisStatsCall(call, builtin);
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
    case ast::Builtin::SorterIterSkipRows:
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
    case ast::Builtin::CSVReaderInit:
    case ast::Builtin::CSVReaderAdvance:
    case ast::Builtin::CSVReaderGetField:
    case ast::Builtin::CSVReaderGetRecordNumber:
    case ast::Builtin::CSVReaderClose: {
      VisitCSVReaderCall(call, builtin);
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
    case ast::Builtin::Ctlz:
    case ast::Builtin::Cttz: {
      VisitBuiltinBitsCall(call, builtin);
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
      Visit(call->GetArguments()[1]);
      break;
    }
    case ast::Builtin::IntCast: {
      VisitBuiltinIntCastCall(call);
      break;
    }
  }
}

void BytecodeGenerator::VisitRegularCallExpression(ast::CallExpression *call) {
  // All function invocation parameters.
  // Reserve now to save re-allocations.
  std::vector<LocalVar> params;
  params.reserve(call->NumArgs());

  // If the function has a non-nil return, the first parameter must be a pointer
  // to where the result is written. If the caller wants the result of the
  // invocation, it must have provided where to store the result. Otherwise, we
  // need to allocate a temporary local.

  const auto func_type = call->GetFunction()->GetType()->As<ast::FunctionType>();

  if (!func_type->GetReturnType()->IsNilType()) {
    LocalVar ret_val;
    if (bool caller_wants_result = GetExecutionResult() != nullptr; caller_wants_result) {
      ret_val = GetExecutionResult()->GetOrCreateDestination(func_type->GetReturnType());
      if (GetExecutionResult()->IsRValue()) {
        GetExecutionResult()->SetDestination(ret_val.ValueOf());
      }
    } else {
      ret_val = GetCurrentFunction()->NewLocal(func_type->GetReturnType());
    }
    params.push_back(ret_val);
  }

  // Collect non-return-value parameters as usual.
  for (uint32_t i = 0; i < func_type->GetNumParams(); i++) {
    params.push_back(VisitExpressionForRValue(call->GetArguments()[i]));
  }

  // Emit call.
  const auto func_id = LookupFuncIdByName(call->GetFuncName().ToString());
  TPL_ASSERT(func_id != kInvalidFuncId, "Function not found!");
  GetEmitter()->EmitCall(func_id, params);
}

void BytecodeGenerator::VisitCallExpression(ast::CallExpression *node) {
  const auto call_kind = node->GetCallKind();
  if (call_kind == ast::CallExpression::CallKind::Builtin) {
    VisitBuiltinCallExpression(node);
  } else {
    VisitRegularCallExpression(node);
  }
}

void BytecodeGenerator::VisitAssignmentStatement(ast::AssignmentStatement *node) {
  LocalVar dest = VisitExpressionForLValue(node->GetDestination());
  VisitExpressionForRValue(node->GetSource(), dest);
}

void BytecodeGenerator::VisitFile(ast::File *node) {
  for (auto *decl : node->GetDeclarations()) {
    Visit(decl);
  }
}

void BytecodeGenerator::VisitLiteralExpression(ast::LiteralExpression *node) {
  TPL_ASSERT(GetExecutionResult()->IsRValue(), "Literal expressions cannot be R-Values!");

  LocalVar target = GetExecutionResult()->GetOrCreateDestination(node->GetType());

  switch (node->GetLiteralKind()) {
    case ast::LiteralExpression::LiteralKind::Nil: {
      GetEmitter()->EmitAssignImm8(target, 0);
      break;
    }
    case ast::LiteralExpression::LiteralKind::Boolean: {
      GetEmitter()->EmitAssignImm1(target, static_cast<int8_t>(node->BoolVal()));
      GetExecutionResult()->SetDestination(target.ValueOf());
      break;
    }
    case ast::LiteralExpression::LiteralKind::Int: {
      if (const auto size = node->GetType()->GetSize(); size == 1) {
        GetEmitter()->EmitAssignImm1(target, node->IntegerVal());
      } else if (size == 2) {
        GetEmitter()->EmitAssignImm2(target, node->IntegerVal());
      } else if (size == 4) {
        GetEmitter()->EmitAssignImm4(target, node->IntegerVal());
      } else {
        TPL_ASSERT(size == 8, "Invalid integer literal size. Must be 1-, 2-, 4-, or 8-bytes.");
        GetEmitter()->EmitAssignImm8(target, node->IntegerVal());
      }
      GetExecutionResult()->SetDestination(target.ValueOf());
      break;
    }
    case ast::LiteralExpression::LiteralKind::Float: {
      if (const auto size = node->GetType()->GetSize(); size == 4) {
        GetEmitter()->EmitAssignImm4F(target, node->FloatVal());
      } else {
        TPL_ASSERT(size == 8, "Invalid float literal size. Must be 4-, or 8-bytes.");
        GetEmitter()->EmitAssignImm8F(target, node->FloatVal());
      }
      GetExecutionResult()->SetDestination(target.ValueOf());
      break;
    }
    case ast::LiteralExpression::LiteralKind::String: {
      LocalVar string = NewStaticString(node->GetType()->GetContext(), node->StringVal());
      GetEmitter()->EmitAssign(Bytecode::Assign8, target, string);
      GetExecutionResult()->SetDestination(string.ValueOf());
    }
  }
}

void BytecodeGenerator::VisitStructDeclaration(ast::StructDeclaration *) {
  // Nothing to do
}

void BytecodeGenerator::VisitLogicalAndOrExpression(ast::BinaryOpExpression *node) {
  TPL_ASSERT(GetExecutionResult()->IsRValue(), "Binary expressions must be R-Values!");
  TPL_ASSERT(node->GetType()->IsBoolType(), "Boolean binary operation must be of type bool");

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());

  // Execute left child
  VisitExpressionForRValue(node->GetLeft(), dest);

  Bytecode conditional_jump;
  BytecodeLabel end_label;

  switch (node->Op()) {
    case parsing::Token::Type::OR: {
      conditional_jump = Bytecode::JumpIfTrue;
      break;
    }
    case parsing::Token::Type::AND: {
      conditional_jump = Bytecode::JumpIfFalse;
      break;
    }
    default: {
      UNREACHABLE("Impossible logical operation type");
    }
  }

  // Do a conditional jump
  GetEmitter()->EmitConditionalJump(conditional_jump, dest.ValueOf(), &end_label);

  // Execute the right child
  VisitExpressionForRValue(node->GetRight(), dest);

  // Bind the end label
  GetEmitter()->Bind(&end_label);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

#define MATH_BYTECODE(CODE_RESULT, MATH_OP, TPL_TYPE)                                           \
  if (TPL_TYPE->IsIntegerType()) {                                                              \
    CODE_RESULT = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::MATH_OP), TPL_TYPE);     \
  } else {                                                                                      \
    TPL_ASSERT(TPL_TYPE->IsFloatType(), "Only integer and floating point math operations");     \
    CODE_RESULT = GetFloatTypedBytecode(GET_BASE_FOR_FLOAT_TYPES(Bytecode::MATH_OP), TPL_TYPE); \
  }

void BytecodeGenerator::VisitPrimitiveArithmeticExpression(ast::BinaryOpExpression *node) {
  TPL_ASSERT(GetExecutionResult()->IsRValue(), "Arithmetic expressions must be R-Values!");

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
  LocalVar left = VisitExpressionForRValue(node->GetLeft());
  LocalVar right = VisitExpressionForRValue(node->GetRight());

  Bytecode bytecode;
  switch (node->Op()) {
    case parsing::Token::Type::PLUS: {
      MATH_BYTECODE(bytecode, Add, node->GetType());
      break;
    }
    case parsing::Token::Type::MINUS: {
      MATH_BYTECODE(bytecode, Sub, node->GetType());
      break;
    }
    case parsing::Token::Type::STAR: {
      MATH_BYTECODE(bytecode, Mul, node->GetType());
      break;
    }
    case parsing::Token::Type::SLASH: {
      MATH_BYTECODE(bytecode, Div, node->GetType());
      break;
    }
    case parsing::Token::Type::PERCENT: {
      MATH_BYTECODE(bytecode, Rem, node->GetType());
      break;
    }
    case parsing::Token::Type::AMPERSAND: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitAnd), node->GetType());
      break;
    }
    case parsing::Token::Type::BIT_OR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitOr), node->GetType());
      break;
    }
    case parsing::Token::Type::BIT_XOR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitXor), node->GetType());
      break;
    }
    case parsing::Token::Type::BIT_SHL: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitShl), node->GetType());
      break;
    }
    case parsing::Token::Type::BIT_SHR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitShr), node->GetType());
      break;
    }
    default: {
      UNREACHABLE("Impossible binary operation");
    }
  }

  // Emit
  GetEmitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

#undef MATH_BYTECODE

void BytecodeGenerator::VisitSqlArithmeticExpression(ast::BinaryOpExpression *node) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
  LocalVar left = VisitExpressionForSQLValue(node->GetLeft());
  LocalVar right = VisitExpressionForSQLValue(node->GetRight());

  const bool is_integer_math = node->GetType()->IsSpecificBuiltin(ast::BuiltinType::IntegerVal);

  Bytecode bytecode;
  switch (node->Op()) {
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
    default: {
      UNREACHABLE("Impossible arithmetic SQL operation");
    }
  }

  // Emit
  GetEmitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest);
}

void BytecodeGenerator::VisitArithmeticExpression(ast::BinaryOpExpression *node) {
  if (node->GetType()->IsSqlValueType()) {
    VisitSqlArithmeticExpression(node);
  } else {
    VisitPrimitiveArithmeticExpression(node);
  }
}

void BytecodeGenerator::VisitBinaryOpExpression(ast::BinaryOpExpression *node) {
  switch (node->Op()) {
    case parsing::Token::Type::AND:
    case parsing::Token::Type::OR: {
      VisitLogicalAndOrExpression(node);
      break;
    }
    default: {
      VisitArithmeticExpression(node);
      break;
    }
  }
}

#define SQL_COMPARISON_BYTECODE(CODE_RESULT, COMPARISON_TYPE, ARG_KIND) \
  switch (ARG_KIND) {                                                   \
    case ast::BuiltinType::Kind::BooleanVal:                            \
      CODE_RESULT = Bytecode::COMPARISON_TYPE##Bool;                    \
      break;                                                            \
    case ast::BuiltinType::Kind::IntegerVal:                            \
      CODE_RESULT = Bytecode::COMPARISON_TYPE##Integer;                 \
      break;                                                            \
    case ast::BuiltinType::Kind::RealVal:                               \
      CODE_RESULT = Bytecode::COMPARISON_TYPE##Real;                    \
      break;                                                            \
    case ast::BuiltinType::Kind::DateVal:                               \
      CODE_RESULT = Bytecode::COMPARISON_TYPE##Date;                    \
      break;                                                            \
    case ast::BuiltinType::Kind::StringVal:                             \
      CODE_RESULT = Bytecode::COMPARISON_TYPE##String;                  \
      break;                                                            \
    default:                                                            \
      UNREACHABLE("Undefined SQL comparison!");                         \
  }

void BytecodeGenerator::VisitSqlComparisonExpression(ast::ComparisonOpExpression *compare) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(compare->GetType());
  LocalVar left = VisitExpressionForSQLValue(compare->GetLeft());
  LocalVar right = VisitExpressionForSQLValue(compare->GetRight());

  TPL_ASSERT(compare->GetLeft()->GetType() == compare->GetRight()->GetType(),
             "Left and right input types to comparison are not equal");

  const auto arg_kind = compare->GetLeft()->GetType()->As<ast::BuiltinType>()->GetKind();

  Bytecode code;
  switch (compare->Op()) {
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
    default: {
      UNREACHABLE("Impossible binary operation");
    }
  }

  // Emit
  GetEmitter()->EmitBinaryOp(code, dest, left, right);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest);
}

#undef SQL_COMPARISON_BYTECODE

#define COMPARISON_BYTECODE(CODE_RESULT, COMPARISON_TYPE, TPL_TYPE)                              \
  if (TPL_TYPE->IsIntegerType()) {                                                               \
    CODE_RESULT =                                                                                \
        GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::COMPARISON_TYPE), TPL_TYPE);        \
  } else if (TPL_TYPE->IsFloatType()) {                                                          \
    CODE_RESULT =                                                                                \
        GetFloatTypedBytecode(GET_BASE_FOR_FLOAT_TYPES(Bytecode::COMPARISON_TYPE), TPL_TYPE);    \
  } else {                                                                                       \
    TPL_ASSERT(TPL_TYPE->IsBoolType(), "Only integer, floating point, and boolean comparisons"); \
    CODE_RESULT = Bytecode::COMPARISON_TYPE##_bool;                                              \
  }

void BytecodeGenerator::VisitPrimitiveComparisonExpression(ast::ComparisonOpExpression *compare) {
  TPL_ASSERT(GetExecutionResult()->IsRValue(), "Comparison expressions must be R-Values!");

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(compare->GetType());

  // nil comparison
  if (ast::Expression * input_expr; compare->IsLiteralCompareNil(&input_expr)) {
    LocalVar input = VisitExpressionForRValue(input_expr);
    Bytecode bytecode = compare->Op() == parsing::Token::Type ::EQUAL_EQUAL
                            ? Bytecode::IsNullPtr
                            : Bytecode::IsNotNullPtr;
    GetEmitter()->Emit(bytecode, dest, input);
    GetExecutionResult()->SetDestination(dest.ValueOf());
    return;
  }

  // regular comparison

  TPL_ASSERT(
      compare->GetLeft()->GetType()->IsArithmetic() || compare->GetLeft()->GetType()->IsBoolType(),
      "Invalid type to comparison");
  TPL_ASSERT(compare->GetRight()->GetType()->IsArithmetic() ||
                 compare->GetRight()->GetType()->IsBoolType(),
             "Invalid type to comparison");

  LocalVar left = VisitExpressionForRValue(compare->GetLeft());
  LocalVar right = VisitExpressionForRValue(compare->GetRight());

  Bytecode bytecode;
  switch (compare->Op()) {
    case parsing::Token::Type::GREATER: {
      COMPARISON_BYTECODE(bytecode, GreaterThan, compare->GetLeft()->GetType());
      break;
    }
    case parsing::Token::Type::GREATER_EQUAL: {
      COMPARISON_BYTECODE(bytecode, GreaterThanEqual, compare->GetLeft()->GetType());
      break;
    }
    case parsing::Token::Type::EQUAL_EQUAL: {
      COMPARISON_BYTECODE(bytecode, Equal, compare->GetLeft()->GetType());
      break;
    }
    case parsing::Token::Type::LESS: {
      COMPARISON_BYTECODE(bytecode, LessThan, compare->GetLeft()->GetType());
      break;
    }
    case parsing::Token::Type::LESS_EQUAL: {
      COMPARISON_BYTECODE(bytecode, LessThanEqual, compare->GetLeft()->GetType());
      break;
    }
    case parsing::Token::Type::BANG_EQUAL: {
      COMPARISON_BYTECODE(bytecode, NotEqual, compare->GetLeft()->GetType());
      break;
    }
    default: {
      UNREACHABLE("Impossible binary operation");
    }
  }

  // Emit
  GetEmitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

#undef COMPARISON_BYTECODE

void BytecodeGenerator::VisitComparisonOpExpression(ast::ComparisonOpExpression *node) {
  const bool is_primitive_comparison = node->GetType()->IsSpecificBuiltin(ast::BuiltinType::Bool);
  if (!is_primitive_comparison) {
    VisitSqlComparisonExpression(node);
  } else {
    VisitPrimitiveComparisonExpression(node);
  }
}

void BytecodeGenerator::VisitFunctionLiteralExpression(ast::FunctionLiteralExpression *node) {
  Visit(node->GetBody());
}

void BytecodeGenerator::BuildAssign(LocalVar dest, LocalVar val, ast::Type *dest_type) {
  // Emit the appropriate assignment
  const uint32_t size = dest_type->GetSize();
  if (size == 1) {
    GetEmitter()->EmitAssign(Bytecode::Assign1, dest, val);
  } else if (size == 2) {
    GetEmitter()->EmitAssign(Bytecode::Assign2, dest, val);
  } else if (size == 4) {
    GetEmitter()->EmitAssign(Bytecode::Assign4, dest, val);
  } else if (size == 8) {
    GetEmitter()->EmitAssign(Bytecode::Assign8, dest, val);
  } else {
    GetEmitter()->EmitAssignN(dest, val, size);
  }
}

void BytecodeGenerator::BuildDeref(LocalVar dest, LocalVar ptr, ast::Type *dest_type) {
  // Emit the appropriate deref
  const uint32_t size = dest_type->GetSize();
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

void BytecodeGenerator::VisitMemberExpression(ast::MemberExpression *node) {
  // We first need to compute the address of the object we're selecting into.
  // Thus, we get the L-Value of the object below.

  LocalVar obj_ptr = VisitExpressionForLValue(node->GetObject());

  // We now need to compute the offset of the field in the composite type. TPL
  // unifies C's arrow and dot syntax for field/member access. Thus, the type
  // of the object may be either a pointer to a struct or the actual struct. If
  // the type is a pointer, then the L-Value of the object is actually a double
  // pointer and we need to dereference it; otherwise, we can use the address
  // as is.

  ast::StructType *obj_type = nullptr;
  if (auto *type = node->GetObject()->GetType(); node->IsSugaredArrow()) {
    // Double pointer, need to dereference
    obj_ptr = BuildLoadPointer(obj_ptr, type);
    obj_type = type->As<ast::PointerType>()->GetBase()->As<ast::StructType>();
  } else {
    obj_type = type->As<ast::StructType>();
  }

  // We're now ready to compute offset. Let's lookup the field's offset in the
  // struct type.

  auto field_name = node->GetMember()->As<ast::IdentifierExpression>();
  auto offset = obj_type->GetOffsetOfFieldByName(field_name->GetName());

  // Now that we have a pointer to the composite object, we need to compute a
  // pointer to the field within the object. We do so by generating a LEA op.
  // NOTE: We initially implemented an optimization that elided the LEA if the
  //       field offset was zero. The thinking was that a zero offset meant we
  //       could safely reinterpret cast the object pointer directly into a
  //       pointer to the field since their addresses are equivalent. But, this
  //       blows things up in the backend because we've lost context required to
  //       recognize this optimization. Removing this optimization results in
  //       generating more redundant LEA instructions with zero offsets, which
  //       may impact VM performance and increase backend compilation times
  //       (due to more instructions).
  //       08/19/20 pmenon - Removed optimization. Observed marginal change in
  //                         VM performance and LLVM time in TPC-H benchmark.

  LocalVar field_ptr = GetCurrentFunction()->NewLocal(node->GetType()->PointerTo());
  GetEmitter()->EmitLea(field_ptr, obj_ptr, offset);
  field_ptr = field_ptr.ValueOf();

  if (GetExecutionResult()->IsLValue()) {
    TPL_ASSERT(!GetExecutionResult()->HasDestination(), "L-Values produce their destination");
    GetExecutionResult()->SetDestination(field_ptr);
    return;
  }

  // The caller wants the actual value of the field. We just computed a pointer
  // to the field in the object, so we need to load/dereference it. If the
  // caller provided a destination variable, use that; otherwise, create a new
  // temporary variable to store the value.

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
  BuildDeref(dest, field_ptr, node->GetType());
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitDeclarationStatement(ast::DeclarationStatement *node) {
  Visit(node->GetDeclaration());
}

void BytecodeGenerator::VisitExpressionStatement(ast::ExpressionStatement *node) {
  Visit(node->GetExpression());
}

void BytecodeGenerator::VisitBadExpression(ast::BadExpression *node) {
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
  if (auto *return_type = func_type->GetReturnType(); !return_type->IsNilType()) {
    func->NewParameterLocal(return_type->PointerTo(), "hiddenRv");
  }

  // Register parameters
  for (const auto &param : func_type->GetParams()) {
    func->NewParameterLocal(param.type, param.name.ToString());
  }

  // Cache
  func_map_[func->GetName()] = func->GetId();

  return func;
}

FunctionId BytecodeGenerator::LookupFuncIdByName(const std::string &name) const {
  if (const auto iter = func_map_.find(name); iter != func_map_.end()) {
    return iter->second;
  }
  return kInvalidFuncId;
}

LocalVar BytecodeGenerator::NewStatic(const std::string &name, ast::Type *type,
                                      const void *contents) {
  std::size_t offset = data_.size();

  if (!util::MathUtil::IsAligned(offset, type->GetAlignment())) {
    offset = util::MathUtil::AlignTo(offset, type->GetAlignment());
  }

  const std::size_t padded_len = type->GetSize() + (offset - data_.size());
  data_.insert(data_.end(), padded_len, 0);
  std::memcpy(&data_[offset], contents, type->GetSize());

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
      ast::ArrayType::Get(string.GetLength(), ast::BuiltinType::Get(ctx, ast::BuiltinType::UInt8));
  auto static_local = NewStatic("stringConst", type, static_cast<const void *>(string.GetData()));

  // Cache
  static_string_cache_.emplace(string, static_local);

  return static_local;
}

LocalVar BytecodeGenerator::VisitExpressionForLValue(ast::Expression *expr) {
  LValueResultScope scope(this);
  Visit(expr);
  return scope.GetDestination();
}

LocalVar BytecodeGenerator::VisitExpressionForRValue(ast::Expression *expr) {
  RValueResultScope scope(this);
  Visit(expr);
  return scope.GetDestination();
}

void BytecodeGenerator::VisitExpressionForRValue(ast::Expression *expr, LocalVar dest) {
  RValueResultScope scope(this, dest);
  Visit(expr);
}

LocalVar BytecodeGenerator::VisitExpressionForSQLValue(ast::Expression *expr) {
  return VisitExpressionForLValue(expr);
}

void BytecodeGenerator::VisitExpressionForTest(ast::Expression *expr, BytecodeLabel *then_label,
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

Bytecode BytecodeGenerator::GetIntTypedBytecode(Bytecode bytecode, ast::Type *type, bool sign) {
  TPL_ASSERT(type->IsIntegerType(), "Type must be integer type");
  auto int_kind = type->SafeAs<ast::BuiltinType>()->GetKind();
  auto kind_idx = int_kind - (sign ? ast::BuiltinType::Int8 : ast::BuiltinType::UInt8);
  return Bytecodes::FromByte(Bytecodes::ToByte(bytecode) + kind_idx);
}

Bytecode BytecodeGenerator::GetFloatTypedBytecode(Bytecode bytecode, ast::Type *type) {
  TPL_ASSERT(type->IsFloatType(), "Type must be floating-point type");
  auto float_kind = type->SafeAs<ast::BuiltinType>()->GetKind();
  auto kind_idx = static_cast<uint8_t>(float_kind - ast::BuiltinType::Float32);
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

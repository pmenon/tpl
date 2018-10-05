#include "vm/bytecode_generator.h"

#include "ast/type.h"
#include "logging/logger.h"
#include "util/macros.h"
#include "vm/bytecode_label.h"
#include "vm/bytecode_unit.h"

namespace tpl::vm {

class BytecodeGenerator::ExpressionResultScope {
 public:
  explicit ExpressionResultScope(BytecodeGenerator *generator)
      : generator_(generator),
        destination_(Register::kInvalidIndex),
        outer_scope_(generator->execution_result()) {
    generator_->set_execution_result(this);
  }

  explicit ExpressionResultScope(BytecodeGenerator *generator,
                                 RegisterId destination)
      : generator_(generator),
        destination_(destination),
        outer_scope_(generator->execution_result()) {
    generator_->set_execution_result(this);
  }

  ~ExpressionResultScope() { generator_->set_execution_result(outer_scope_); }

  RegisterId GetOrCreateDestination(ast::Type *type) {
    if (destination_ == Register::kInvalidIndex) {
      destination_ = generator_->curr_func()->NewLocal(type);
    }

    return destination_;
  }

  RegisterId destination() const { return destination_; }
  void set_destination(RegisterId destination) { destination_ = destination; }

 private:
  BytecodeGenerator *generator_;
  RegisterId destination_;
  ExpressionResultScope *outer_scope_;
};

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

void BytecodeGenerator::VisitForStmt(ast::ForStmt *node) {
  AstVisitor::VisitForStmt(node);
}

void BytecodeGenerator::VisitFieldDecl(ast::FieldDecl *node) {
  AstVisitor::VisitFieldDecl(node);
}

void BytecodeGenerator::VisitFunctionDecl(ast::FunctionDecl *node) {
  // Create function info object
  FunctionInfo *func_info = AllocateFunc(node->name().data());

  auto *func_type = node->type_repr()->type()->As<ast::FunctionType>();

  // Register return type
  func_info->NewLocal(func_type->return_type(), "ret", false);

  // Register parameters
  const auto &params = node->type_repr()->parameters();
  const auto &param_types = func_type->params();
  for (u32 i = 0; i < param_types.size(); i++) {
    func_info->NewLocal(param_types[i], params[i]->name().data(), true);
  }

  {
    // Visit the body of the function
    BytecodePositionTracker position_tracker(this, func_info);
    Visit(node->function());
  }
}

void BytecodeGenerator::VisitIdentifierExpr(ast::IdentifierExpr *node) {
  auto reg_id = curr_func()->LookupLocal(node->name().data());
  execution_result()->set_destination(reg_id);
}

void BytecodeGenerator::VisitBlockStmt(ast::BlockStmt *node) {
  for (auto *stmt : node->statements()) {
    Visit(stmt);
  }
}

void BytecodeGenerator::VisitVariableDecl(ast::VariableDecl *node) {
  // Register a new local variale in the function
  auto *type = node->initial() != nullptr ? node->initial()->type()
                                          : node->type_repr()->type();
  RegisterId reg = curr_func()->NewLocal(type, node->name().data(), false);

  // If there's an initializer, handle it now
  if (node->initial() != nullptr) {
    VisitExpressionWithTarget(node->initial(), reg);
  }
}

void BytecodeGenerator::VisitUnaryOpExpr(ast::UnaryOpExpr *node) {
  RegisterId dest = execution_result()->GetOrCreateDestination(node->type());
  RegisterId input = VisitExpressionForValue(node->expr());

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
  emitter()->Emit(bytecode, dest, input);

  // Mark where the result is
  execution_result()->set_destination(dest);
}

void BytecodeGenerator::VisitReturnStmt(ast::ReturnStmt *node) {
  if (node->ret() != nullptr) {
    VisitExpressionWithTarget(node->ret(), curr_func()->GetRVRegister());
  }
  emitter()->EmitReturn();
}

void BytecodeGenerator::VisitCallExpr(ast::CallExpr *node) {
  AstVisitor::VisitCallExpr(node);
}

void BytecodeGenerator::VisitAssignmentStmt(ast::AssignmentStmt *node) {
  RegisterId dest = VisitExpressionForValue(node->destination());
  VisitExpressionWithTarget(node->source(), dest);
}

void BytecodeGenerator::VisitFile(ast::File *node) {
  for (auto *decl : node->declarations()) {
    Visit(decl);
  }
}

void BytecodeGenerator::VisitLitExpr(ast::LitExpr *node) {
  RegisterId target = execution_result()->GetOrCreateDestination(node->type());
  emitter()->EmitLiteral4(target, node->int32_val());
}

void BytecodeGenerator::VisitStructDecl(ast::StructDecl *node) {
  curr_func()->NewLocal(node->type_repr()->type(), node->name().data(), false);
}

void BytecodeGenerator::VisitBinaryOpExpr(ast::BinaryOpExpr *node) {
  RegisterId dest = execution_result()->GetOrCreateDestination(node->type());
  RegisterId left = VisitExpressionForValue(node->left());
  RegisterId right = VisitExpressionForValue(node->right());

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
    case parsing::Token::Type::GREATER: {
      bytecode = GetIntTypedBytecode(
          GET_BASE_FOR_INT_TYPES(Bytecode::GreaterThan), node->type());
      break;
    }
    case parsing::Token::Type::GREATER_EQUAL: {
      bytecode = GetIntTypedBytecode(
          GET_BASE_FOR_INT_TYPES(Bytecode::GreaterThanEqual), node->type());
      break;
    }
    case parsing::Token::Type::EQUAL_EQUAL: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Equal),
                                     node->type());
      break;
    }
    case parsing::Token::Type::LESS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::LessThan),
                                     node->type());
      break;
    }
    case parsing::Token::Type::LESS_EQUAL: {
      bytecode = GetIntTypedBytecode(
          GET_BASE_FOR_INT_TYPES(Bytecode::LessThanEqual), node->type());
      break;
    }
    case parsing::Token::Type::BANG_EQUAL: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::NotEqual),
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
  emitter()->Emit(bytecode, dest, left, right);

  // Mark where the result is
  execution_result()->set_destination(dest);
}

void BytecodeGenerator::VisitFunctionLitExpr(ast::FunctionLitExpr *node) {
  Visit(node->body());
}

void BytecodeGenerator::VisitDeclStmt(ast::DeclStmt *node) {
  Visit(node->declaration());
}

void BytecodeGenerator::VisitIfStmt(ast::IfStmt *node) {
  AstVisitor::VisitIfStmt(node);
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

RegisterId BytecodeGenerator::VisitExpressionForValue(ast::Expr *expr) {
  ExpressionResultScope scope(this);
  Visit(expr);
  return scope.destination();
}

void BytecodeGenerator::VisitExpressionWithTarget(ast::Expr *expr,
                                                  RegisterId reg_id) {
  ExpressionResultScope scope(this, reg_id);
  Visit(expr);
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

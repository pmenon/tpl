#include "vm/bytecode_generator.h"

#include "ast/type.h"
#include "util/macros.h"
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

BytecodeGenerator::BytecodeGenerator() : execution_result_(nullptr) {}

void BytecodeGenerator::VisitForStmt(ast::ForStmt *node) {
  AstVisitor::VisitForStmt(node);
}

void BytecodeGenerator::VisitFieldDecl(ast::FieldDecl *node) {
  AstVisitor::VisitFieldDecl(node);
}

void BytecodeGenerator::VisitFunctionDecl(ast::FunctionDecl *node) {
  // Create function info object
  auto func_id = static_cast<u32>(functions().size());
  FunctionInfo func(func_id);

  auto *func_type = node->type_repr()->type()->As<ast::FunctionType>();

  // Register return type
  func.NewLocal(func_type->return_type(), "ret");

  // Register parameters
  const auto &params = node->type_repr()->parameters();
  const auto &param_types = func_type->params();
  for (u32 i = 0; i < param_types.size(); i++) {
    func.NewLocal(param_types[i], params[i]->name().data());
  }

  // Add new function
  functions_.emplace_back(std::move(func));

  // Generate body
  Visit(node->function());
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
  RegisterId reg =
      curr_func()->NewLocal(node->type_repr()->type(), node->name().data());
  if (node->initial() != nullptr) {
    VisitExpressionWithTarget(node->initial(), reg);
  }
}

void BytecodeGenerator::VisitUnaryOpExpr(ast::UnaryOpExpr *node) {
  AstVisitor::VisitUnaryOpExpr(node);
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
  curr_func()->NewLocal(node->type_repr()->type(), node->name().data());
}

void BytecodeGenerator::VisitBinaryOpExpr(ast::BinaryOpExpr *node) {
  RegisterId dest = execution_result()->GetOrCreateDestination(node->type());
  RegisterId left = VisitExpressionForValue(node->left());
  RegisterId right = VisitExpressionForValue(node->right());

  switch (node->op()) {
    case parsing::Token::Type::PLUS: {
      emitter()->EmitAdd_i32(dest, left, right);
      break;
    }
    case parsing::Token::Type::STAR: {
      emitter()->EmitMul_i32(dest, left, right);
      break;
    }
    default: { break; }
  }
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

std::unique_ptr<BytecodeUnit> BytecodeGenerator::Compile(ast::AstNode *root) {
  BytecodeGenerator generator;
  generator.Visit(root);

  const auto &code = generator.emitter()->Finish();
  const auto &functions = generator.functions();
  return BytecodeUnit::Create(code, functions);
}

}  // namespace tpl::vm

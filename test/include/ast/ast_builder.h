#include <string>
#include <utility>
#include <vector>

#include "ast/ast_node_factory.h"
#include "ast/builtins.h"
#include "ast/context.h"
#include "ast/type.h"
#include "sema/error_reporter.h"
#include "util/region_containers.h"

namespace tpl::ast {

class TestAstBuilder {
 public:
  TestAstBuilder() : error_reporter_(), ctx_(&error_reporter_) {}

  Context *ctx() { return &ctx_; }
  sema::ErrorReporter *error_reporter() { return &error_reporter_; }

  Identifier Ident(const std::string &s) { return ctx()->GetIdentifier(s); }

  Expr *IdentExpr(Identifier ident) { return node_factory()->NewIdentifierExpr(empty_, ident); }

  Expr *IdentExpr(const std::string &s) { return IdentExpr(Ident(s)); }

  Expr *BoolLit(bool b) { return node_factory()->NewBoolLiteral(empty_, b); }

  Expr *IntLit(int32_t i) { return node_factory()->NewIntLiteral(empty_, i); }

  Expr *FloatLit(float i) { return node_factory()->NewFloatLiteral(empty_, i); }

  template <parsing::Token::Type OP>
  Expr *BinOp(Expr *left, Expr *right) {
    return node_factory()->NewBinaryOpExpr(empty_, OP, left, right);
  }

  template <parsing::Token::Type OP>
  Expr *Cmp(Expr *left, Expr *right) {
    TPL_ASSERT(parsing::Token::IsCompareOp(OP), "Not a comparison");
    return node_factory()->NewComparisonOpExpr(empty_, OP, left, right);
  }

  Expr *CmpEq(Expr *left, Expr *right) {
    return Cmp<parsing::Token::Type::EQUAL_EQUAL>(left, right);
  }
  Expr *CmpNe(Expr *left, Expr *right) {
    return Cmp<parsing::Token::Type::BANG_EQUAL>(left, right);
  }
  Expr *CmpLt(Expr *left, Expr *right) { return Cmp<parsing::Token::Type::LESS>(left, right); }

  Expr *Field(Expr *obj, Expr *field) { return node_factory()->NewMemberExpr(empty_, obj, field); }

  VariableDecl *DeclVar(Identifier name, Expr *init) { return DeclVar(name, nullptr, init); }

  VariableDecl *DeclVar(Identifier name, Expr *type_repr, Expr *init) {
    return node_factory()->NewVariableDecl(empty_, name, type_repr, init);
  }

  Expr *DeclRef(Decl *decl) { return IdentExpr(decl->name()); }

  Stmt *DeclStmt(Decl *decl) { return node_factory()->NewDeclStmt(decl); }

  Stmt *Block(std::initializer_list<Stmt *> stmts) {
    util::RegionVector<Stmt *> region_stmts(stmts.begin(), stmts.end(), ctx()->region());
    return node_factory()->NewBlockStmt(empty_, empty_, std::move(region_stmts));
  }

  Stmt *ExprStmt(Expr *expr) { return node_factory()->NewExpressionStmt(expr); }

  Expr *PtrType(Expr *base) { return node_factory()->NewPointerType(empty_, base); }

  template <BuiltinType::Kind BUILTIN>
  Expr *BuiltinTypeRepr() {
    return IdentExpr(BuiltinType::Get(ctx(), BUILTIN)->tpl_name());
  }

  Expr *PrimIntTypeRepr() { return BuiltinTypeRepr<BuiltinType::Int32>(); }
  Expr *PrimFloatTypeRepr() { return BuiltinTypeRepr<BuiltinType::Float32>(); }
  Expr *PrimBoolTypeRepr() { return BuiltinTypeRepr<BuiltinType::Bool>(); }

  Expr *IntegerSqlTypeRepr() { return BuiltinTypeRepr<BuiltinType::Integer>(); }
  Expr *RealSqlTypeRepr() { return BuiltinTypeRepr<BuiltinType::Real>(); }
  Expr *StringSqlTypeRepr() { return BuiltinTypeRepr<BuiltinType::StringVal>(); }

  Expr *ArrayTypeRepr(Expr *type) { return node_factory()->NewArrayType(empty_, nullptr, type); }

  Expr *ArrayIndex(Expr *arr, Expr *idx) { return node_factory()->NewIndexExpr(empty_, arr, idx); }

  template <Builtin BUILTIN, typename... Args>
  CallExpr *Call(Args... args) {
    auto fn = IdentExpr(Builtins::GetFunctionName(BUILTIN));
    auto call_args = util::RegionVector<Expr *>({std::forward<Args>(args)...}, ctx()->region());
    return node_factory()->NewBuiltinCallExpr(fn, std::move(call_args));
  }

 private:
  AstNodeFactory *node_factory() { return ctx()->node_factory(); }

 private:
  sema::ErrorReporter error_reporter_;
  Context ctx_;
  SourcePosition empty_{0, 0};
};

}  // namespace tpl::ast

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

  Expression *IdentExpr(Identifier ident) {
    return node_factory()->NewIdentifierExpression(empty_, ident);
  }

  Expression *IdentExpr(const std::string &s) { return IdentExpr(Ident(s)); }

  Expression *BoolLit(bool b) { return node_factory()->NewBoolLiteral(empty_, b); }

  Expression *IntLit(int64_t i) { return node_factory()->NewIntLiteral(empty_, i); }

  Expression *FloatLit(float i) { return node_factory()->NewFloatLiteral(empty_, i); }

  template <parsing::Token::Type OP>
  Expression *BinOp(Expression *left, Expression *right) {
    return node_factory()->NewBinaryOpExpression(empty_, OP, left, right);
  }

  template <parsing::Token::Type OP>
  Expression *Cmp(Expression *left, Expression *right) {
    TPL_ASSERT(parsing::Token::IsCompareOp(OP), "Not a comparison");
    return node_factory()->NewComparisonOpExpression(empty_, OP, left, right);
  }

  Expression *CmpEq(Expression *left, Expression *right) {
    return Cmp<parsing::Token::Type::EQUAL_EQUAL>(left, right);
  }
  Expression *CmpNe(Expression *left, Expression *right) {
    return Cmp<parsing::Token::Type::BANG_EQUAL>(left, right);
  }
  Expression *CmpLt(Expression *left, Expression *right) {
    return Cmp<parsing::Token::Type::LESS>(left, right);
  }

  Expression *Field(Expression *obj, Expression *field) {
    return node_factory()->NewMemberExpression(empty_, obj, field);
  }

  VariableDeclaration *DeclVar(Identifier name, Expression *init) {
    return DeclVar(name, nullptr, init);
  }

  VariableDeclaration *DeclVar(std::string n, Expression *init) {
    return DeclVar(Ident(n), nullptr, init);
  }

  VariableDeclaration *DeclVar(std::string n, std::string type_name, Expression *init) {
    return DeclVar(Ident(n), IdentExpr(type_name), init);
  }

  VariableDeclaration *DeclVar(Identifier name, Expression *type_repr, Expression *init) {
    return node_factory()->NewVariableDeclaration(empty_, name, type_repr, init);
  }

  FieldDeclaration *GenFieldDecl(Identifier name, ast::Expression *type_repr) {
    return node_factory()->NewFieldDeclaration(empty_, name, type_repr);
  }

  StructDeclaration *DeclStruct(Identifier name,
                                std::initializer_list<ast::FieldDeclaration *> fields) {
    util::RegionVector<FieldDeclaration *> f(fields.begin(), fields.end(), ctx()->GetRegion());
    ast::StructTypeRepr *type = node_factory()->NewStructType(empty_, std::move(f));
    return node_factory()->NewStructDeclaration(empty_, name, type);
  }

  Expression *DeclRef(Declaration *decl) { return IdentExpr(decl->GetName()); }

  Statement *DeclStmt(Declaration *decl) { return node_factory()->NewDeclStatement(decl); }

  Statement *Block(std::initializer_list<Statement *> stmts) {
    util::RegionVector<Statement *> region_stmts(stmts.begin(), stmts.end(), ctx()->GetRegion());
    return node_factory()->NewBlockStatement(empty_, empty_, std::move(region_stmts));
  }

  Statement *ExprStmt(Expression *expr) { return node_factory()->NewExpressionStatement(expr); }

  Expression *PtrType(Expression *base) { return node_factory()->NewPointerType(empty_, base); }

  template <BuiltinType::Kind BUILTIN>
  Expression *BuiltinTypeRepr() {
    return IdentExpr(BuiltinType::Get(ctx(), BUILTIN)->GetTplName());
  }

  Expression *PrimIntTypeRepr() { return BuiltinTypeRepr<BuiltinType::Int32>(); }
  Expression *PrimFloatTypeRepr() { return BuiltinTypeRepr<BuiltinType::Float32>(); }
  Expression *PrimBoolTypeRepr() { return BuiltinTypeRepr<BuiltinType::Bool>(); }

  Expression *IntegerSqlTypeRepr() { return BuiltinTypeRepr<BuiltinType::IntegerVal>(); }
  Expression *RealSqlTypeRepr() { return BuiltinTypeRepr<BuiltinType::RealVal>(); }
  Expression *StringSqlTypeRepr() { return BuiltinTypeRepr<BuiltinType::StringVal>(); }

  Expression *ArrayTypeRepr(Expression *type) {
    return node_factory()->NewArrayType(empty_, nullptr, type);
  }
  Expression *ArrayTypeRepr(Expression *type, uint32_t len) {
    return node_factory()->NewArrayType(empty_, IntLit(len), type);
  }

  Expression *ArrayIndex(Expression *arr, Expression *idx) {
    return node_factory()->NewIndexExpression(empty_, arr, idx);
  }

  template <Builtin BUILTIN, typename... Args>
  CallExpression *Call(Args... args) {
    auto fn = IdentExpr(Builtins::GetFunctionName(BUILTIN));
    auto call_args =
        util::RegionVector<Expression *>({std::forward<Args>(args)...}, ctx()->GetRegion());
    return node_factory()->NewBuiltinCallExpression(fn, std::move(call_args));
  }

  File *GenFile(std::initializer_list<ast::Declaration *> decls) {
    util::RegionVector<Declaration *> d(decls.begin(), decls.end(), ctx()->GetRegion());
    return node_factory()->NewFile(empty_, std::move(d));
  }

 private:
  AstNodeFactory *node_factory() { return ctx()->GetNodeFactory(); }

 private:
  sema::ErrorReporter error_reporter_;
  Context ctx_;
  SourcePosition empty_{0, 0};
};

}  // namespace tpl::ast

#include "vm/rewriter.h"

#include "llvm/ADT/StringRef.h"

#include "ast/ast.h"
#include "ast/ast_node_factory.h"
#include "ast/ast_traversal_visitor.h"
#include "ast/builtins.h"
#include "ast/context.h"

namespace tpl::vm {

namespace {

class RewriteScans : public ast::AstTraversalVisitor<RewriteScans> {
 public:
  explicit RewriteScans(ast::Context *ctx, ast::AstNode *root)
      : ast::AstTraversalVisitor<RewriteScans>(root), ctx_(ctx) {
    (void)ctx_;
  }

  void VisitBlockStmt(ast::BlockStmt *node);

 private:
  ast::Context *ctx_;
};

void RewriteScans::VisitBlockStmt(ast::BlockStmt *node) {
#if 0
  for (u32 idx = 0; idx < node->statements().size(); idx++) {
    auto *for_stmt = node->statements()[idx]->SafeAs<ast::ForInStmt>();
    if (for_stmt == nullptr) {
      continue;
    }

    auto *builtin_call = for_stmt->iter()->SafeAs<ast::CallExpr>();
    if (builtin_call == nullptr ||
        builtin_call->call_kind() != ast::CallExpr::CallKind::Builtin) {
      continue;
    }

    const auto table_iterate_name = llvm::StringRef(
        ast::Builtins::GetFunctionName(ast::Builtin::TableIterate));
    if (table_iterate_name == builtin_call->GetFuncName().data()) {
      node->statements()[idx] = RewriteScan(for_stmt);
    }
  }
#endif
  // Recurse into the block's statements
  AstTraversalVisitor<RewriteScans>::Visit(node);
}

}  // namespace

void RewriteForInScans(ast::Context *ctx, ast::AstNode *root) {
  RewriteScans rewrite_scans(ctx, root);
  rewrite_scans.Run();
}

}  // namespace tpl::vm

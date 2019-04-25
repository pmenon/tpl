#include "compiler/code_builder.h"

namespace tpl::compiler {
Block *CodeBlock::Call(Function *fn, std::initializer_list<Value *> arguments) {
  util::RegionVector<ast::Expr *> args(region_.get());
  for (auto a : arguments) {
    args.push_back(a->GetExpr());
  }
  auto retBlock =
      nodeFactory_->NewCallExpr(fn->GetIdentifierExpr(), std::move(args));
  blocks_.emplace_back(retBlock);
  return retBlock;
}

Block *CodeBlock::CreateForInLoop(Value *target, Value *iter, CodeBlock *body,
                                  bool batch) {
  SourcePosition dummy;
  ast::Stmt *retStmt;
  if (batch) {
    util::Region mapRegion("mapregion");
    util::RegionUnorderedMap<ast::Identifier, ast::Expr *> map(&mapRegion);
    ast::Identifier batchIdent("batch");

    // TODO (tanujnay112) make the value an actual Expr*
    map.emplace(std::move(batchIdent), nullptr);
    ast::Attributes attrib(std::move(map));
    retStmt = nodeFactory_->NewForInStmt(
        dummy, target->GetExpr(), iter->GetExpr(), &attrib, body->Compile());
  } else {
    retStmt = nodeFactory_->NewForInStmt(
        dummy, target->GetExpr(), iter->GetExpr(), nullptr, body->Compile());
  }
  blocks_.push_back(retStmt);
  return retStmt;
}

}  // namespace tpl::compiler
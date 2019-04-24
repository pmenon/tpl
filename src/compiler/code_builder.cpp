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
}  // namespace tpl::compiler
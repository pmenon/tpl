//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// loop.h
//
// Identification: src/include/codegen/lang/loop.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <compiler/value/TPLValue.h>
#include <vector>

#include "compiler/codegen.h"
#include "compiler/value.h"

namespace tpl {
namespace compiler {
namespace lang {

//===----------------------------------------------------------------------===//
// A utility class to help generate loops in TPL
//===----------------------------------------------------------------------===//
/*class Loop {
 public:
  explicit Loop(CodeBlock *body) : body_(body){};

  virtual ast::IterationStmt *Compile(ast::AstNodeFactory &factory) = 0;

  CodeBlock *GetBody(){return body_};

 private:
  CodeBlock *body_;
};

class ForInLoop : public Loop {
 public:
  explicit ForInLoop(Value *target, Value *iter, CodeBlock *body, bool batch)
      : Loop(body), target_(target), iter_(iter), batch_(batch){};

  ast::IterationStmt *Compile(ast::AstNodeFactory &factory) override {
    SourcePosition dummy;
    if (batch_) {
      util::Region mapRegion("mapregion");
      util::RegionUnorderedMap<ast::Identifier, ast::Expr *> map(&mapRegion);
      ast::Identifier batchIdent("batch");

      //TODO (tanujnay112) make the value an actual Expr*
      map.emplace(std::move(batchIdent), nullptr);
      ast::Attributes attrib(std::move(map));
      return factory.NewForInStmt(dummy, target_->GetExpr(), iter_->GetExpr(),
                                  &attrib, GetBody()->Compile());
    }
    return factory.NewForInStmt(dummy, target_->GetExpr(), iter_->GetExpr(),
                                nullptr, GetBody()->Compile());
  }

 private:
  Value *target_;
  Value *iter_;
  bool batch_;
};*/

}  // namespace lang
}  // namespace compiler
}  // namespace tpl
#pragma once

#include <unordered_map>
#include "llvm/ADT/DenseMap.h"

#include "ast/context.h"
#include "ast/identifier.h"
#include "util/common.h"

namespace tpl::parsing {

class ParsingContext {
 public:
  ParsingContext() : outer_(nullptr), scope_level_(0) {}

  ParsingContext NewNestedContext() {
    return ParsingContext(this, scope_level_ + 1);
  }

  bool SymbolExists(ast::Identifier sym) {
    for (const ParsingContext *ctx = this; ctx != nullptr; ctx = ctx->outer_) {
      auto iter = ctx->symbols_.find(sym);
      if (iter != ctx->symbols_.end()) {
        return true;
      }
    }
    return false;
  }

  ast::Identifier GetScopedSymbol(ast::Identifier sym) {
    for (const ParsingContext *ctx = this; ctx != nullptr; ctx = ctx->outer_) {
      auto iter = ctx->symbols_.find(sym);
      if (iter != ctx->symbols_.end()) {
        return iter->second;
      }
    }
    TPL_ASSERT(false, "Where did the symbol go?");
    return sym;
  }

  ast::Identifier MakeUniqueSymbol(ast::Context *ctx, ast::Identifier sym) {
    // if the symbol doesn't exist, we're good. map the symbol to itself.
    if (!SymbolExists(sym)) {
      symbols_.insert({sym, sym});
      return sym;
    }
    // otherwise, we must make a new symbol.
    auto new_sym_name = new std::string(sym.data());
    new_sym_name->append(std::to_string(scope_level_));
    auto new_sym = ctx->GetIdentifier(*new_sym_name);
    symbols_.erase(sym);
    symbols_.insert({sym, new_sym});
    return new_sym;
  }

 private:
  ParsingContext(ParsingContext *outer, u8 scope_level)
      : outer_(outer), scope_level_(scope_level) {}

  llvm::DenseMap<ast::Identifier, ast::Identifier> symbols_;
  ParsingContext *outer_;
  u8 scope_level_;
};

}  // namespace tpl::parsing
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

  bool SymbolNameExists(const std::string &sym_name) {
    for (const ParsingContext *ctx = this; ctx != nullptr; ctx = ctx->outer_) {
      auto iter = ctx->symbols_.find(sym_name);
      if (iter != ctx->symbols_.end()) {
        return true;
      }
    }
    return false;
  }

  const std::string &GetScopedSymbolName(const std::string &sym_name) {
    for (const ParsingContext *ctx = this; ctx != nullptr; ctx = ctx->outer_) {
      auto iter = ctx->symbols_.find(sym_name);
      if (iter != ctx->symbols_.end()) {
        return iter->second;
      }
    }
    TPL_ASSERT(false, "No name?");
    return sym_name;
  }

  const std::string &MakeUniqueSymbolName(const std::string &sym_name) {
    // if the symbol doesn't exist, we're good. map the symbol to itself.
    if (!SymbolNameExists(sym_name)) {
      symbols_[sym_name] = sym_name;
      return sym_name;
    }
    // otherwise, we must make a new symbol.
    // TODO(WAN): more efficient way?
    auto new_sym_name = new std::string(sym_name);
    new_sym_name->append(std::to_string(scope_level_));
    symbols_[sym_name] = *new_sym_name;
    return *new_sym_name;
  }

 private:
  ParsingContext(ParsingContext *outer, u8 scope_level)
      : outer_(outer), scope_level_(scope_level) {}

  llvm::StringMap<std::string> symbols_;
  ParsingContext *outer_;
  u8 scope_level_;
};

}  // namespace tpl::parsing
#pragma once

#include <string>
#include <unordered_map>

#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/DenseSet.h"

#include "ast/context.h"
#include "ast/identifier.h"
#include "util/common.h"

namespace tpl::parsing {

/**
 * ParsingContext manages symbols across the entire parse tree.
 */
class ParsingContext {
 public:
  /**
   * Creates a new empty parsing context with no parent.
   */
  ParsingContext() : root_known_symbols_(&known_symbols_), outer_(nullptr) {}

  /**
   * @return a new child of the current context
   */
  ParsingContext NewNestedContext() {
    return ParsingContext(root_known_symbols_, this);
  }

  /**
   * Check if the symbol exists ANYWHERE in the root ParsingContext tree.
   * @param sym symbol that we're looking for
   * @return true if the symbol exists
   */
  bool SymbolExists(ast::Identifier sym) {
    auto iter = root_known_symbols_->find(sym);
    return iter != root_known_symbols_->end();
  }

  /**
   * Returns the scoped version of the given identifier.
   * @param sym original version of the identifier
   * @return scoped version of the given identifier
   */
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
    // TPL does not allow re-declaring variables in the same scope.
    // We can support this easily by removing the below check.

    // If the symbol is already within this scope, we just return it.
    auto iter = symbols_.find(sym);
    if (iter != symbols_.end()) {
      return iter->second;
    }

    // If the symbol doesn't exist anywhere, declare it here.
    if (!SymbolExists(sym)) {
      symbols_.insert({sym, sym});
      root_known_symbols_->insert(sym);
      return sym;
    }

    // Otherwise, a new unique symbol must be created.
    // TODO(WAN): the string appends probably suck for performance.

    // We append the number of known symbols to our identifier since that
    // guarantees no conflict between our own generated variables.
    const std::string num_symbols_str =
        "_tpl" + std::to_string(root_known_symbols_->size());

    std::string new_sym_name(sym.data());
    new_sym_name.append(num_symbols_str);

    auto new_sym = ctx->GetIdentifier(new_sym_name);
    while (SymbolExists(new_sym)) {
      new_sym_name.append(num_symbols_str);
      new_sym = ctx->GetIdentifier(new_sym_name);
    }

    symbols_.erase(sym);
    symbols_.insert({sym, new_sym});
    root_known_symbols_->insert(new_sym);
    return new_sym;
  }

 private:
  /**
   * Creates a new ParsingContext with the given outer scope and scope level.
   * @param outer Outer context that encapsulates this parsing context
   * @param scope_level The scope level of the current parsing context
   */
  ParsingContext(llvm::DenseSet<ast::Identifier> *root_known_symbols,
                 ParsingContext *outer)
      : root_known_symbols_(root_known_symbols), outer_(outer) {}

  /* In the root parsing context. */
  llvm::DenseSet<ast::Identifier> known_symbols_;
  /* In all children's parsing context. */
  llvm::DenseSet<ast::Identifier> *root_known_symbols_;

  /* Symbols in this scope. */
  llvm::DenseMap<ast::Identifier, ast::Identifier> symbols_;
  /* Outer scope. */
  ParsingContext *outer_;
};

}  // namespace tpl::parsing


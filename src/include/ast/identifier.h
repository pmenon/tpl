#pragma once

#include <functional>

#include "llvm/ADT/DenseMapInfo.h"

#include "common/macros.h"

namespace tpl::ast {

/**
 * A uniqued string identifier in some AST context. This serves as a super-lightweight string
 * reference. Two identifiers allocated from the same AST context are equal if they have the same
 * pointer value - we don't need to check contents.
 */
class Identifier {
 public:
  explicit Identifier(const char *str) noexcept : data_(str) {}

  const char *data() const { return data_; }

  std::size_t length() const {
    TPL_ASSERT(data_ != nullptr, "Trying to get the length of an invalid identifier");
    return std::strlen(data());
  }

  bool empty() const { return length() == 0; }

  bool operator==(const Identifier &other) const { return data() == other.data(); }

  bool operator!=(const Identifier &other) const { return !(*this == other); }

  static Identifier GetEmptyKey() {
    return Identifier(static_cast<const char *>(llvm::DenseMapInfo<const void *>::getEmptyKey()));
  }

  static Identifier GetTombstoneKey() {
    return Identifier(
        static_cast<const char *>(llvm::DenseMapInfo<const void *>::getTombstoneKey()));
  }

 private:
  const char *data_;
};

}  // namespace tpl::ast

namespace llvm {

/**
 * Functor to make Identifiers usable from LLVM DenseMap.
 */
template <>
struct DenseMapInfo<tpl::ast::Identifier> {
  static tpl::ast::Identifier getEmptyKey() { return tpl::ast::Identifier::GetEmptyKey(); }

  static tpl::ast::Identifier getTombstoneKey() { return tpl::ast::Identifier::GetTombstoneKey(); }

  static unsigned getHashValue(const tpl::ast::Identifier identifier) {
    return DenseMapInfo<const void *>::getHashValue(static_cast<const void *>(identifier.data()));
  }

  static bool isEqual(const tpl::ast::Identifier lhs, const tpl::ast::Identifier rhs) {
    return lhs == rhs;
  }
};

}  // namespace llvm

namespace std {

/**
 * Functor to make Identifiers usable as keys in STL/TPL maps.
 */
template <>
struct hash<tpl::ast::Identifier> {
  std::size_t operator()(const tpl::ast::Identifier &identifier) const noexcept {
    return std::hash<const char *>()(identifier.data());
  }
};

}  // namespace std

#pragma once

#include <functional>
#include <string_view>

#include "llvm/ADT/DenseMapInfo.h"

#include "common/macros.h"

namespace tpl::ast {

/**
 * A uniqued string identifier in some AST context. This serves as a super-lightweight string
 * reference. Two identifiers allocated from the same AST context are equal if they have the same
 * pointer value - we don't need to check contents.
 */
class Identifier {
 private:
  friend class Context;

  // Constructor accessible only to Context which ensures uniqueness.
  explicit Identifier(const char *str) noexcept : data_(str) {}

 public:
  /**
   * Create an empty identifier.
   */
  Identifier() noexcept : data_(nullptr) {}

  /**
   * @return A const pointer to this identifier's underlying string data.
   */
  const char *GetData() const noexcept { return data_; }

  /**
   * @return The length of this identifier in bytes.
   */
  std::size_t GetLength() const {
    TPL_ASSERT(data_ != nullptr, "Trying to get the length of an invalid identifier");
    return std::strlen(GetData());
  }

  /**
   * @return True if this identifier is empty; false otherwise.
   */
  bool IsEmpty() const noexcept { return data_ == nullptr; }

  /**
   * @return A string view over this identifier.
   */
  std::string_view GetString() const noexcept { return std::string_view(data_, GetLength()); }

  /**
   * Is this identifier equal to another identifier @em other.
   * @param other The identifier to compare with.
   * @return True if equal; false otherwise.
   */
  bool operator==(const Identifier &other) const noexcept { return GetData() == other.GetData(); }

  /**
   * Is this identifier not equal to another identifier @em other.
   * @param other The identifier to compare with.
   * @return True if not equal; false otherwise.
   */
  bool operator!=(const Identifier &other) const noexcept { return !(*this == other); }

  /**
   * @return An identifier that can be used to indicate an empty idenfitier.
   */
  static Identifier GetEmptyKey() {
    return Identifier(static_cast<const char *>(llvm::DenseMapInfo<const void *>::getEmptyKey()));
  }

  /**
   * @return A tombstone key that can be used to determine deleted keys in LLVM's DenseMap. This
   *         Identifier cannot equal any valid identifier that can be stored in the map.
   */
  static Identifier GetTombstoneKey() {
    return Identifier(
        static_cast<const char *>(llvm::DenseMapInfo<const void *>::getTombstoneKey()));
  }

 private:
  // Data
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
    return DenseMapInfo<const void *>::getHashValue(
        static_cast<const void *>(identifier.GetData()));
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
    return std::hash<const char *>()(identifier.GetData());
  }
};

}  // namespace std

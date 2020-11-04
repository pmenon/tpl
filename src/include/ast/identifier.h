#pragma once

#include <functional>
#include <string>
#include <string_view>

#include "llvm/ADT/DenseMapInfo.h"
#include "llvm/ADT/StringMapEntry.h"

#include "common/macros.h"

namespace tpl::ast {

/**
 * A uniqued string identifier in some AST context. This serves as a super-lightweight string
 * reference. Two identifiers allocated from the same AST context are equal if they have the same
 * pointer value - we needn't check contents.
 */
class Identifier {
  using EntryType = llvm::StringMapEntry<llvm::NoneType>;

 private:
  friend class Context;

  // Constructor accessible only to Context which ensures uniqueness.
  // Given the string entry stored in context's identifier table.
  explicit Identifier(const EntryType *entry) noexcept : entry_(entry) {}

 public:
  /**
   * Create an empty identifier.
   */
  Identifier() noexcept : entry_(nullptr) {}

  /**
   * Copy-construct the given identifier. This is a very light-weight operation.
   * @param other The identifier to copy.
   */
  Identifier(const Identifier &other) = default;

  /**
   * Copy-assign the given identifier. This is a very light-weight operation.
   * @param other The identifier to copy.
   */
  Identifier &operator=(const Identifier &other) = default;

  /**
   * @return A const pointer to this identifier's underlying string data.
   */
  const char *GetData() const noexcept { return entry_->getKeyData(); }

  /**
   * @return The length of this identifier in bytes.
   */
  std::size_t GetLength() const noexcept { return entry_->getKeyLength(); }

  /**
   * @return True if this identifier is empty; false otherwise.
   */
  bool IsEmpty() const noexcept { return entry_ == nullptr; }

  /**
   * @return A string view over this identifier.
   */
  std::string_view GetView() const noexcept { return entry_->getKey(); }

  /**
   * @return A copy of this identifier's contents as a string.
   */
  std::string ToString() const { return entry_->getKey().str(); }

  /**
   * Allows implicit conversions of Identifiers to string-views.
   * @return String-view representation of this identifier.
   */
  operator std::string_view() const noexcept { return GetView(); }

  /**
   * @return True if this identifier is equal to @em that; false otherwise.
   */
  bool operator==(const Identifier &that) const noexcept { return entry_ == that.entry_; }

  /**
   * @return True if this identifier is NOT equal to @em that; false otherwise.
   */
  bool operator!=(const Identifier &that) const noexcept { return !(*this == that); }

  /**
   * @return An identifier that can be used to indicate an available entry in a DenseMap.
   */
  static Identifier GetEmptyKey() {
    return Identifier(
        static_cast<const EntryType *>(llvm::DenseMapInfo<const void *>::getEmptyKey()));
  }

  /**
   * @return A tombstone key that can be used to determine deleted keys in LLVM's DenseMap. This
   *         Identifier cannot equal any valid identifier that can be stored in the map.
   */
  static Identifier GetTombstoneKey() {
    return Identifier(
        static_cast<const EntryType *>(llvm::DenseMapInfo<const void *>::getTombstoneKey()));
  }

 private:
  // Pointer to the map entry storing the string.
  const EntryType *entry_;
};

/**
 * @return True if the left Identifier is equivalent to the right string; false otherwise.
 */
inline bool operator==(Identifier lhs, std::string_view rhs) { return lhs.GetView() == rhs; }

/**
 * @return True if the left Identifier is **NOT** equivalent to the right string; false otherwise.
 */
inline bool operator!=(Identifier lhs, std::string_view rhs) { return !(lhs == rhs); }

/**
 * @return True if the left string is equivalent to the right Identifier; false otherwise.
 */
inline bool operator==(std::string_view lhs, Identifier rhs) { return rhs.GetView() == lhs; }

/**
 * @return True if the left string is **NOT** equivalent to the right Identifier; false otherwise.
 */
inline bool operator!=(std::string_view lhs, Identifier rhs) { return !(lhs == rhs); }

}  // namespace tpl::ast

namespace llvm {

/**
 * Functor to make Identifiers usable from LLVM DenseMap.
 */
template <>
struct DenseMapInfo<tpl::ast::Identifier> {
  /** @return An identifier representing an empty key. */
  static tpl::ast::Identifier getEmptyKey() { return tpl::ast::Identifier::GetEmptyKey(); }

  /** @return An identifier representing a deleted key. */
  static tpl::ast::Identifier getTombstoneKey() { return tpl::ast::Identifier::GetTombstoneKey(); }

  /** @return The hash of the given identifier. */
  static unsigned getHashValue(const tpl::ast::Identifier identifier) {
    return DenseMapInfo<const void *>::getHashValue(
        static_cast<const void *>(identifier.GetData()));
  }

  /** @return True if the given identifiers are equal; false otherwise. */
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
  /** @return The hash of the given identifier. */
  std::size_t operator()(const tpl::ast::Identifier &identifier) const noexcept {
    return std::hash<const char *>()(identifier.GetData());
  }
};

}  // namespace std

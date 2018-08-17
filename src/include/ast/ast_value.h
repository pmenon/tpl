#pragma once

#include <cstdint>
#include <cstring>

#include "util/hash.h"
#include "util/hash_map.h"
#include "util/region.h"

namespace tpl::ast {

class AstString : public util::RegionObject {
 public:
  const char *bytes() const { return bytes_; }
  uint32_t length() const { return len_; }
  uint32_t hash_val() const { return hash_; }

  static bool Compare(const AstString *lhs, const AstString *rhs) {
    if (lhs->length() != rhs->length()) {
      return false;
    }

    return (memcmp(lhs->bytes(), rhs->bytes(), lhs->length()) == 0);
  }

 private:
  friend class AstStringsContainer;

  AstString(const char *bytes, uint32_t len, uint32_t hash)
      : bytes_(bytes), len_(len), hash_(hash) {
    TPL_ASSERT(bytes != nullptr);
    TPL_ASSERT(len > 0);
  }

 private:
  // Bytes of the string, memory owned by region
  const char *bytes_;
  uint32_t len_;

  // The hash of the string (used for hashing)
  uint32_t hash_;
};

class AstStringsContainer {
 public:
  explicit AstStringsContainer(util::Region &region)
      : region_(region), string_table_(CompareString()) {}

  DISALLOW_COPY_AND_MOVE(AstStringsContainer);

  AstString *GetAstString(const char *bytes, uint32_t len) {
    const uint32_t hash = util::Hasher::Hash(bytes, len);
    AstString key(bytes, len, hash);

    auto *entry = string_table_.LookupOrInsert(&key, key.hash_val());
    if (entry->value == nullptr) {
      // The entry is new, let's copy over the bytes into the region
      auto *copy = region_.AllocateArray<char>(len);
      std::memcpy(copy, bytes, len);
      entry->key = new (region_) AstString(copy, len, hash);
      entry->value = reinterpret_cast<void *>(1);
    }
    return static_cast<AstString *>(entry->key);
  }

  AstString *GetAstString(const std::string &s) {
    return GetAstString(s.data(), static_cast<uint32_t>(s.length()));
  }

 private:
  util::Region &region_;

  struct CompareString {
    bool operator()(const AstString *lhs, const AstString *rhs) const noexcept {
      return AstString::Compare(lhs, rhs);
    }
  };

  util::SimpleHashMap<AstString *, void *, CompareString> string_table_;
};

}  // namespace tpl::ast
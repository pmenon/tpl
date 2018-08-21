#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <utility>

#include "util/hash.h"
#include "util/macros.h"
#include "util/string_ref.h"

namespace tpl::util {

template <typename Value, typename Allocator>
class StringMap {
 public:
  static constexpr const uint32_t kDefaultInitialCapacity = 16;

  explicit StringMap(uint32_t capacity = kDefaultInitialCapacity,
                     Allocator allocator = Allocator())
      : allocator_(allocator) {
    Init(capacity);
  }

  ~StringMap() {
    for (size_t i = 0; i < capacity_; i++) {
      StringMapEntry *entry = table_[i];
      if (entry != nullptr && entry != TombstoneEntry()) {
        entry->Destroy(allocator());
      }
    }

    size_t allocated = capacity() * (sizeof(StringMapEntry) + sizeof(uint32_t));
    allocator().deallocate(reinterpret_cast<char *>(table_), allocated);
  }

  // Forward declare
  class StringMapEntry;
  class StringMapIterator;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Main public API - try to mimic STL
  ///
  //////////////////////////////////////////////////////////////////////////////

  StringMapIterator find(StringRef key) const {
    uint32_t bucket_num = 0;
    if (!FindKey(key, bucket_num)) return end();
    return StringMapIterator(table_ + bucket_num, true);
  }

  std::pair<StringMapIterator, bool> insert(
      std::pair<StringRef, Value> key_value) {
    return emplace(key_value.first, std::move(key_value.second));
  }

  template <typename... ArgTypes>
  std::pair<StringMapIterator, bool> emplace(StringRef key,
                                             ArgTypes &&... vals) {
    uint32_t bucket_num = 0;
    if (!AcquireBucketFor(key, bucket_num)) {
      return {StringMapIterator(table_ + bucket_num, true), false};
    }

    if (table_[bucket_num] == TombstoneEntry()) {
      num_tomestones_--;
    }

    table_[bucket_num] = StringMapEntry::Create(
        key, allocator(), std::forward<ArgTypes>(vals)...);

    num_elems_++;

    bucket_num = RehashIfNeeded(bucket_num);

    return {StringMapIterator(table_ + bucket_num, true), true};
  }

  bool erase(StringRef key) {
    auto *removed_entry = RemoveKey(key);
    if (removed_entry == nullptr) return false;

    removed_entry->Destroy(allocator());
    return true;
  }

  /**
   * An entry in the hash table
   */
  class StringMapEntry {
   public:
    const char *key() const { return key_; }
    char *key() { return key_; }

    uint64_t key_length() const { return length_; }

    const Value &value() const { return value_; }

    template <typename... ArgTypes>
    static StringMapEntry *Create(StringRef key, Allocator &allocator,
                                  ArgTypes &&... vals) {
      uint64_t key_len = key.length();

      // Allocate a new entry
      size_t alloc_size = sizeof(StringMapEntry) + key_len + 1;

      auto *entry =
          reinterpret_cast<StringMapEntry *>(allocator.allocate(alloc_size));

      // Initialize it
      new (entry) StringMapEntry(key_len, std::forward<ArgTypes>(vals)...);

      // Copy key bytes and NULL terminate
      char *entry_key = entry->key();
      std::memcpy(reinterpret_cast<void *>(entry_key), key.data(), key_len);
      entry_key[key_len] = 0;

      return entry;
    }

    void Destroy(Allocator &allocator) {
      this->~StringMapEntry();
      size_t allocated = sizeof(StringMapEntry) + key_length() + 1;
      allocator.deallocate(reinterpret_cast<char *>(this), allocated);
    }

   private:
    template <typename... ArgTypes>
    StringMapEntry(uint64_t length, ArgTypes &&... vals)
        : length_(length), value_(std::forward<ArgTypes>(vals)...) {}

   private:
    uint64_t length_;
    Value value_;
    char key_[0];
  };

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  static StringMapEntry *TombstoneEntry() {
    static constexpr const intptr_t kInvalidBucket = -1;
    return reinterpret_cast<StringMapEntry *>(kInvalidBucket);
  }

  uint64_t num_elems() const { return num_elems_; }
  uint64_t num_tombstones() const { return num_tomestones_; }
  uint64_t capacity() const { return capacity_; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Iterators
  ///
  //////////////////////////////////////////////////////////////////////////////

  class StringMapIterator {
   public:
    explicit StringMapIterator(StringMapEntry **entry, bool advance = true)
        : entry_(entry) {
      if (advance) Next();
    }

    StringMapIterator &operator=(const StringMapIterator &other) {
      entry_ == other.entry_;
      return *this;
    }

    bool operator==(const StringMapIterator &other) const {
      return entry_ == other.entry_;
    }

    bool operator!=(const StringMapIterator &other) const {
      return !(*this == other);
    }

    // Pre-increment
    StringMapIterator &operator++() {
      ++entry_;
      Next();
      return *this;
    }

    // Post-increment
    StringMapIterator operator++(int) {
      StringMapIterator temp(entry_);
      ++*this;
      return temp;
    }

    const StringMapEntry &operator*() const { return *entry_; }

    const StringMapEntry *operator->() const { return *entry_; }

   private:
    void Next() {
      while (*entry_ == nullptr || *entry_ == StringMap::TombstoneEntry()) {
        entry_++;
      }
    }

   private:
    StringMapEntry **entry_;
  };

  StringMapIterator begin() { return StringMapIterator(table_, true); }
  StringMapIterator begin() const { return StringMapIterator(table_, true); }

  StringMapIterator end() {
    return StringMapIterator(table_ + capacity_, false);
  }

  StringMapIterator end() const {
    return StringMapIterator(table_ + capacity_, false);
  }

 private:
  Allocator &allocator() { return allocator_; }

  // Initialize the entire hash table
  void Init(uint64_t capacity);

  // Looks up the given key in the hash table. If the key is found, the output
  // bucket number parameter is set to the correct bucket position and the
  // function returns true. If the key is not found, the function returns false.
  bool FindKey(StringRef key, uint32_t &bucket_num) const;

  // Acquire a bucket for the given key. If the key already exists, the function
  // returns false. If the key is new, the function returns true, the output
  // bucket number parameter is set and the key's hash value is inserted into
  // the hashes table portion of the table.
  bool AcquireBucketFor(StringRef key, uint32_t &bucket_num);

  // Remove the entry with the given key from this table, returning it to the
  // caller. Fill a tombstone in it's place.
  StringMapEntry *RemoveKey(StringRef key);

  // Grow the hash table if needed
  uint32_t RehashIfNeeded(uint32_t input_bucket_num);

 private:
  Allocator allocator_;

  StringMapEntry **table_;
  uint64_t num_elems_;
  uint64_t num_tomestones_;
  uint64_t capacity_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// StringMap implementation below
///
////////////////////////////////////////////////////////////////////////////////

template <typename Value, typename Allocator>
void StringMap<Value, Allocator>::Init(uint64_t capacity) {
  num_elems_ = 0;

  num_tomestones_ = 0;

  capacity_ = capacity;

  size_t alloc_size = capacity_ * (sizeof(StringMapEntry *) + sizeof(uint32_t));

  table_ =
      reinterpret_cast<StringMapEntry **>(allocator().allocate(alloc_size));

  for (size_t i = 0; i < capacity; i++) {
    table_[i] = nullptr;
  }
}

template <typename Value, typename Allocator>
bool StringMap<Value, Allocator>::FindKey(StringRef key,
                                          uint32_t &bucket_num) const {
  const uint32_t *hashes_table =
      reinterpret_cast<uint32_t *>(table_ + capacity_);

  uint32_t hash = Hasher::Hash(key.data(), key.length());

  bucket_num = hash & (capacity_ - 1);

  uint32_t jump = 1;
  while (true) {
    StringMapEntry *entry = table_[bucket_num];

    if (entry == nullptr) {
      return false;
    }

    if (entry == TombstoneEntry()) {
      // continue
    } else if (hash == hashes_table[bucket_num] &&
               key == StringRef(entry->key(), entry->key_length())) {
      // Match!
      return true;
    }

    // Continue
    bucket_num = (bucket_num + jump) & (capacity_ - 1);

    jump++;
  }
}

template <typename Value, typename Allocator>
bool StringMap<Value, Allocator>::AcquireBucketFor(StringRef key,
                                                   uint32_t &bucket_num) {
  auto *hashes_table = reinterpret_cast<uint32_t *>(table_ + capacity_);

  uint32_t hash = Hasher::Hash(key.data(), key.length());

  bucket_num = hash & (capacity_ - 1);

  uint32_t jump = 1;
  int first_tombstone = -1;
  while (true) {
    StringMapEntry *entry = table_[bucket_num];

    if (entry == nullptr) {
      if (first_tombstone != -1) {
        // We saw a tombstone along the way, take it
        bucket_num = static_cast<uint32_t>(first_tombstone);
      }

      hashes_table[bucket_num] = hash;
      return true;
    }

    if (entry == TombstoneEntry()) {
      // Continue, but first track if this is the first tombstone
      if (first_tombstone == -1) {
        first_tombstone = bucket_num;
      }
    } else if (hash == hashes_table[bucket_num] &&
               key == StringRef(entry->key(), entry->key_length())) {
      // Existing key, can't acquire
      return false;
    }

    // Continue
    bucket_num = (bucket_num + jump) & (capacity_ - 1);

    jump++;
  }
}

template <typename Value, typename Allocator>
typename StringMap<Value, Allocator>::StringMapEntry *
StringMap<Value, Allocator>::RemoveKey(StringRef key) {
  uint32_t bucket_num = 0;
  if (!FindKey(key, bucket_num)) return nullptr;

  auto *entry = table_[bucket_num];
  table_[bucket_num] = TombstoneEntry();
  num_elems_--;
  num_tomestones_++;

  return entry;
}

template <typename Value, typename Allocator>
uint32_t StringMap<Value, Allocator>::RehashIfNeeded(
    uint32_t input_bucket_num) {
  // If the table is more than 75% full, or if the table has fewer than 1/8
  // entries, rehash.

  uint64_t new_cap;
  if (TPL_UNLIKELY(num_elems() * 4 > capacity() * 3)) {
    new_cap = capacity() * 2;
  } else if (TPL_UNLIKELY(capacity() - (num_elems() + num_tombstones()) <=
                          capacity() / 8)) {
    new_cap = capacity();
  } else {
    // Nothing to do
    return input_bucket_num;
  }

  // Allocate the new table
  size_t alloc_size = new_cap * (sizeof(StringMapEntry *) + sizeof(uint32_t));
  auto *new_tab =
      reinterpret_cast<StringMapEntry **>(allocator().allocate(alloc_size));
  for (uint32_t i = 0; i < new_cap; i++) {
    new_tab[i] = nullptr;
  }

  // Hashes table
  auto *hashes_table = reinterpret_cast<uint32_t *>(table_ + capacity());
  auto *new_hashes_table = reinterpret_cast<uint32_t *>(new_tab + new_cap);

  uint32_t new_bucket_num = input_bucket_num;

  // Move everything over
  for (size_t i = 0; i < capacity(); i++) {
    StringMapEntry *entry = table_[i];

    if (entry == nullptr || entry == TombstoneEntry()) {
      continue;
    }

    uint32_t hash = hashes_table[i];
    uint32_t bucket_num = hash & (new_cap - 1);

    StringMapEntry *new_entry_slot = new_tab[bucket_num];
    if (new_entry_slot == nullptr) {
      // Fast path
      new_tab[bucket_num] = entry;
      new_hashes_table[bucket_num] = hash;
      if (i == input_bucket_num) {
        new_bucket_num = bucket_num;
      }
      continue;
    }

    // Slow path, probe
    uint32_t jump = 1;
    do {
      bucket_num = (bucket_num + jump++) & (new_cap - 1);
    } while (new_tab[bucket_num] != nullptr);

    new_tab[bucket_num] = entry;
    new_hashes_table[bucket_num] = hash;
    if (i == input_bucket_num) {
      new_bucket_num = bucket_num;
    }
  }

  // Free the old table, swap in new one
  size_t old_alloc_size =
      capacity() * (sizeof(StringMapEntry) + sizeof(uint32_t));
  allocator().deallocate(reinterpret_cast<char *>(table_), old_alloc_size);

  // Setup new table
  table_ = new_tab;
  capacity_ = new_cap;

  // We've cleaned up all tombstone, so reset
  num_tomestones_ = 0;

  return new_bucket_num;
}

}  // namespace tpl::util
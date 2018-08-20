#pragma once

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <type_traits>

#include "util/macros.h"

namespace tpl::util {

template <typename Key, typename Value>
struct HashMapEntry;

template <typename Key, typename Value>
struct HashMapEntry {
  Key key;
  Value value;
  uint32_t hash;

  HashMapEntry(Key key, Value value, uint32_t hash)
      : key(key), value(value), hash(hash), occupied_(true) {}

  bool occupied() const { return occupied_; }

  void clear() { occupied_ = false; }

 private:
  bool occupied_;
};

template <typename Key>
struct HashMapEntry<Key, void> {
  Key key;
  uint32_t hash;

  HashMapEntry(Key key, uint32_t hash)
      : key(key), hash(hash), occupied_(true) {}

  bool occupied() const { return occupied_; }

  void clear() { occupied_ = false; }

 private:
  bool occupied_;
};

/**
 * A generic, non-multithreaded, resizable, open-addressing hash table with
 * linear probing. For small key-value pairs, this is much faster (2x) than
 * std::unordered_map
 */
template <typename Key, typename Value, typename MatchFunc,
          typename Allocator = std::allocator<char>>
class HashMap {
  static_assert(std::is_invocable_r_v<bool, MatchFunc, uint32_t, uint32_t,
                                      const Key &, const Key &>);

 public:
  using Entry = HashMapEntry<Key, Value>;

  static constexpr const uint32_t kDefaultInitialCapacity = 8;

  // Construct the hash map with some capacity and using some match function
  explicit HashMap(uint32_t capacity = kDefaultInitialCapacity,
                   MatchFunc match = MatchFunc(),
                   Allocator allocator = Allocator());

  ~HashMap();

  // Lookup the given key (with its hash value) in this map. If found, return
  // the entry. If not found, return null.
  Entry *Lookup(const Key &key, uint32_t hash);

  // Lookup the given key (with its hash value) in this map. If found, return
  // the entry. If not found, insert the given value into the map.
  Entry *LookupOrInsert(const Key &key, uint32_t hash);

  // Insert a new key value pair into this map
  Entry *Insert(const Key &key, const Value &val, uint32_t hash);

  // Remove the entry with the given key (and hash). If exits, return the value
  // associated with the key. If the key doesn't exist, return null.
  Value Remove(const Key &key, uint32_t hash);

  // Iteration
  using Iterator = Entry *;
  using ConstIterator = const Entry *;
  Iterator begin() { return map_; }
  ConstIterator begin() const { return map_; }
  Iterator end() { return map_ + capacity_; }
  ConstIterator end() const { return map_ + capacity_; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  uint64_t occupancy() const { return occupancy_; }

  uint64_t occupancy_threshold() const { return occupancy_threshold_; }

  uint64_t capacity() const { return capacity_; }

 private:
  // Initialize the hash table
  void Initialize(uint64_t capacity);

  // Return the entry in the map where the given key (and hash) is found, or
  // where it should be placed if a match doesn't exist
  Entry *Probe(const Key &key, uint32_t hash) const;

  Entry *FillEntry(Entry *entry, const Key &key, const Value &val,
                   uint32_t hash);

  // Resize the map
  void Resize();

 private:
  Entry *map_;
  uint64_t occupancy_;
  uint64_t occupancy_threshold_;
  uint64_t capacity_;

  MatchFunc match_;
  Allocator allocator_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// HashMap implementation below
///
////////////////////////////////////////////////////////////////////////////////

template <typename Key, typename Value, typename MatchFunc, typename Allocator>
HashMap<Key, Value, MatchFunc, Allocator>::HashMap(uint32_t capacity,
                                                   MatchFunc match,
                                                   Allocator allocator)
    : map_(nullptr),
      occupancy_(0),
      occupancy_threshold_(0),
      capacity_(0),
      match_(match),
      allocator_(allocator) {
  Initialize(capacity);
}

template <typename Key, typename Value, typename MatchFunc, typename Allocator>
HashMap<Key, Value, MatchFunc, Allocator>::~HashMap() {
  if (map_ != nullptr) {
    const size_t size = capacity_ * sizeof(Entry);
    allocator_.deallocate(reinterpret_cast<char *>(map_), size);
    map_ = nullptr;
  }
}

template <typename Key, typename Value, typename MatchFunc, typename Allocator>
inline void HashMap<Key, Value, MatchFunc, Allocator>::Initialize(
    uint64_t capacity) {
  occupancy_ = 0;

  capacity_ = capacity;

  occupancy_threshold_ = 3 * (capacity_ / 4);

  map_ =
      reinterpret_cast<Entry *>(allocator_.allocate(capacity_ * sizeof(Entry)));

  for (uint64_t i = 0; i < capacity_; i++) {
    map_[i].clear();
  }
}

template <typename Key, typename Value, typename MatchFunc, typename Allocator>
inline typename HashMap<Key, Value, MatchFunc, Allocator>::Entry *
HashMap<Key, Value, MatchFunc, Allocator>::Probe(const Key &key,
                                                 uint32_t hash) const {
  // This assertion ensures we quit the loop below
  TPL_ASSERT(occupancy_ < capacity_);

  uint64_t idx = (hash & (capacity_ - 1));

  while (map_[idx].occupied() &&
         !match_(map_[idx].hash, hash, map_[idx].key, key)) {
    idx = (idx + 1) & (capacity_ - 1);
  }

  return &map_[idx];
}

template <typename Key, typename Value, typename MatchFunc, typename Allocator>
inline typename HashMap<Key, Value, MatchFunc, Allocator>::Entry *
HashMap<Key, Value, MatchFunc, Allocator>::FillEntry(HashMap::Entry *entry,
                                                     const Key &key,
                                                     const Value &val,
                                                     uint32_t hash) {
  TPL_ASSERT(!entry->occupied());

  new (entry) Entry(key, val, hash);
  occupancy_++;

  if (occupancy_ > occupancy_threshold_) {
    Resize();

    // TODO: We can avoid this re-probe if we track position during resizing
    entry = Probe(key, hash);
  }

  return entry;
}

template <typename Key, typename Value, typename MatchFunc, typename Allocator>
void HashMap<Key, Value, MatchFunc, Allocator>::Resize() {
  Entry *old = map_;
  uint64_t occ = occupancy_;
  uint64_t old_cap = capacity_;

  // Initialize new map with double the capacity
  Initialize(capacity_ * 2);

  // Setup new table
  for (Entry *entry = old; occ > 0; entry++) {
    if (entry->occupied()) {
      Entry *new_entry = Probe(entry->key, entry->hash);
      FillEntry(new_entry, entry->key, entry->value, entry->hash);
      occ--;
    }
  }

  // Free old map
  allocator_.deallocate(reinterpret_cast<char *>(old), old_cap * sizeof(Entry));
}

template <typename Key, typename Value, typename MatchFunc, typename Allocator>
inline typename HashMap<Key, Value, MatchFunc, Allocator>::Entry *
HashMap<Key, Value, MatchFunc, Allocator>::Lookup(const Key &key,
                                                  uint32_t hash) {
  Entry *entry = Probe(key, hash);
  return (entry->occupied() ? entry : nullptr);
}

template <typename Key, typename Value, typename MatchFunc, typename Allocator>
inline typename HashMap<Key, Value, MatchFunc, Allocator>::Entry *
HashMap<Key, Value, MatchFunc, Allocator>::LookupOrInsert(const Key &key,
                                                          uint32_t hash) {
  Entry *entry = Probe(key, hash);

  if (entry->occupied()) {
    return entry;
  }

  return FillEntry(entry, key, Value(), hash);
}

template <typename Key, typename Value, typename MatchFunc, typename Allocator>
inline typename HashMap<Key, Value, MatchFunc, Allocator>::Entry *
HashMap<Key, Value, MatchFunc, Allocator>::Insert(const Key &key,
                                                  const Value &val,
                                                  uint32_t hash) {
  Entry *entry = Probe(key, hash);
  TPL_ASSERT(!entry->occupied());
  return FillEntry(entry, key, val, hash);
}

template <typename Key, typename Value, typename MatchFunc, typename Allocator>
inline Value HashMap<Key, Value, MatchFunc, Allocator>::Remove(const Key &key,
                                                               uint32_t hash) {
  Entry *a = Probe(key, hash);
  if (!a->occupied()) {
    return nullptr;
  }

  Value val = a->value;

  // Deletion logic from: https://en.wikipedia.org/wiki/Open_addressing

  Entry *b = a;
  while (true) {
    b = b + 1;
    if (b == end()) {
      b = map_;
    }

    if (!b->occupied()) {
      break;
    }

    Entry *c = map_ + (b->hash & (capacity_ - 1));

    if ((a < b && (a <= c || b < c)) || (a > b && (c <= a && c > b))) {
      *a = *b;
      a = b;
    }
  }

  a->clear();
  occupancy_--;
  return val;
}

////////////////////////////////////////////////////////////////////////////////
///
/// Key matching adapters
///
////////////////////////////////////////////////////////////////////////////////

/**
 * A functor that only relies on key equality
 */
template <typename Key>
struct MatchKeyOnly {
  bool operator()(UNUSED uint32_t hash1, UNUSED uint32_t hash2, const Key key1,
                  const Key &key2) const {
    return key1 == key2;
  }
};

/**
 * A matcher that first compares hashes before perform a potentially more
 * complex key comparison operation.
 */
template <typename Key, typename KeyEq>
struct MatchHashThenKey {
  static_assert(std::is_invocable_r_v<bool, KeyEq, const Key &, const Key &>);

  KeyEq key_eq_;

  explicit MatchHashThenKey(KeyEq key_eq) : key_eq_(key_eq) {}

  bool operator()(uint32_t hash1, uint32_t hash2, const Key key1,
                  const Key &key2) const {
    return hash1 == hash2 && key_eq_(key1, key2);
  }
};

////////////////////////////////////////////////////////////////////////////////
///
/// Common hash map specializations
///
////////////////////////////////////////////////////////////////////////////////

/**
 * A simple generic hash map that behaves similarly to std::unordered_map
 */
template <typename Key, typename Value, typename KeyEq = std::equal_to<Key>,
          typename Allocator = std::allocator<char>>
class SimpleHashMap
    : public HashMap<Key, Value, MatchHashThenKey<Key, KeyEq>, Allocator> {
 public:
  using BaseMap = HashMap<Key, Value, MatchHashThenKey<Key, KeyEq>, Allocator>;

  explicit SimpleHashMap(KeyEq key_eq)
      : BaseMap(BaseMap::kDefaultInitialCapacity,
                MatchHashThenKey<Key, KeyEq>(key_eq)) {}
};

/**
 *
 */
template <typename Key, typename Value,
          typename Allocator = std::allocator<char>>
class PointerHashMap
    : public HashMap<Key, Value, MatchKeyOnly<Key>, Allocator> {
  static_assert(
      std::is_pointer_v<Key>,
      "The key type to this hash map implementation must be a pointer type");

 public:
  using BaseMap = HashMap<Key, Value, MatchKeyOnly<Key>, Allocator>;

  explicit PointerHashMap()
      : BaseMap(BaseMap::kDefaultInitialCapacity, MatchKeyOnly<Key>()) {}
};

}  // namespace tpl::util
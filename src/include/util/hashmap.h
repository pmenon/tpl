#pragma once

#include <cstdint>
#include <cstdlib>
#include <type_traits>

#include "util/macros.h"

namespace tpl::util {

/**
 * A generic resizable, single-threaded, open-addressing hash table. Inspired by
 * V8.
 *
 * @tparam Key The type of the key stored in the table
 * @tparam Value The type of the value
 * @tparam MatchFunc
 */
template <typename Key, typename Value, typename MatchFunc>
class HashMap {
  static_assert(std::is_invocable_r_v<bool, MatchFunc, uint32_t, uint32_t,
                                      const Key &, const Key &>);

 public:
  /**
   * An entry in the hash map
   */
  struct Entry {
    Key key;
    Value value;
    uint32_t hash;

    Entry(Key key, Value value, uint32_t hash)
        : key(key), value(value), hash(hash), occupied_(true) {}

    bool occupied() const { return occupied_; }

    void clear() { occupied_ = false; }

   private:
    bool occupied_;
  };

  static constexpr const uint32_t kDefaultInitialCapacity = 8;

  // Construct the hash map with some capacity and using some match function
  HashMap(uint32_t capacity = kDefaultInitialCapacity,
          MatchFunc match = MatchFunc());

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
};

////////////////////////////////////////////////////////////////////////////////
///
/// HashMap implementation below
///
////////////////////////////////////////////////////////////////////////////////

template <typename Key, typename Value, typename MatchFunc>
HashMap<Key, Value, MatchFunc>::HashMap(uint32_t capacity, MatchFunc match)
    : map_(nullptr), occupancy_(0), match_(match) {
  Initialize(capacity);
}

template <typename Key, typename Value, typename MatchFunc>
HashMap<Key, Value, MatchFunc>::~HashMap() {
  if (map_ != nullptr) {
    free(map_);
    map_ = nullptr;
  }
}

template <typename Key, typename Value, typename MatchFunc>
inline void HashMap<Key, Value, MatchFunc>::Initialize(uint64_t capacity) {
  capacity_ = capacity;

  occupancy_threshold_ = 3 * (capacity_ / 4);

  map_ = static_cast<Entry *>(malloc(capacity_ * sizeof(Entry)));

  for (uint64_t i = 0; i < capacity_; i++) {
    map_[i].clear();
  }
}

template <typename Key, typename Value, typename MatchFunc>
inline typename HashMap<Key, Value, MatchFunc>::Entry *
HashMap<Key, Value, MatchFunc>::Probe(const Key &key, uint32_t hash) const {
  // This assertion ensures we quit the loop below
  TPL_ASSERT(occupancy_ < capacity_);

  uint64_t idx = (hash & (capacity_ - 1));

  while (map_[idx].occupied() &&
         !match_(map_[idx].hash, hash, map_[idx].key, key)) {
    idx = (idx + 1) & (capacity_ - 1);
  }

  return &map_[idx];
}

template <typename Key, typename Value, typename MatchFunc>
inline typename HashMap<Key, Value, MatchFunc>::Entry *
HashMap<Key, Value, MatchFunc>::FillEntry(HashMap::Entry *entry, const Key &key,
                                          const Value &val, uint32_t hash) {
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

template <typename Key, typename Value, typename MatchFunc>
void HashMap<Key, Value, MatchFunc>::Resize() {
  Entry *old = map_;
  uint64_t occ = occupancy_;

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
  free(old);
}

template <typename Key, typename Value, typename MatchFunc>
inline typename HashMap<Key, Value, MatchFunc>::Entry *
HashMap<Key, Value, MatchFunc>::Lookup(const Key &key, uint32_t hash) {
  Entry *entry = Probe(key, hash);
  return (entry->occupied() ? entry : nullptr);
}

template <typename Key, typename Value, typename MatchFunc>
inline typename HashMap<Key, Value, MatchFunc>::Entry *
HashMap<Key, Value, MatchFunc>::LookupOrInsert(const Key &key, uint32_t hash) {
  Entry *entry = Probe(key, hash);

  if (entry->occupied()) {
    return entry;
  }

  return FillEntry(entry, key, Value(), hash);
}

template <typename Key, typename Value, typename MatchFunc>
inline typename HashMap<Key, Value, MatchFunc>::Entry *
HashMap<Key, Value, MatchFunc>::Insert(const Key &key, const Value &val,
                                       uint32_t hash) {
  Entry *entry = Probe(key, hash);
  TPL_ASSERT(!entry->occupied());
  return FillEntry(entry, key, val, hash);
}

template <typename Key, typename Value, typename MatchFunc>
inline Value HashMap<Key, Value, MatchFunc>::Remove(const Key &key,
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
/// Simple hash map
///
////////////////////////////////////////////////////////////////////////////////

/**
 * This functor adapts a full-blown hash value and key matcher into one that
 * early-exists on hash value mismatch first, then forwards to an injected
 * key-only equality check.
 *
 * @tparam Key
 * @tparam MatchFunc
 */
template <typename Key, typename MatchFunc>
struct MatchHashThenKey {
  static_assert(
      std::is_invocable_r_v<bool, MatchFunc, const Key &, const Key &>);

  MatchFunc match_;

  explicit MatchHashThenKey(MatchFunc match) : match_(match) {}

  bool operator()(uint32_t hash1, uint32_t hash2, const Key key1,
                  const Key &key2) const {
    return hash1 == hash2 && match_(key1, key2);
  }
};

/**
 * A generic hash map that uses void pointers for keys and values, along with an
 * injected key-only equality checker functor
 */
class SimpleHashMap
    : public HashMap<void *, void *,
                     MatchHashThenKey<void *, bool (*)(void *, void *)>> {
 public:
  using MatchFunc = bool (*)(void *, void *);

  using BaseMap = HashMap<void *, void *, MatchHashThenKey<void *, MatchFunc>>;

  explicit SimpleHashMap(MatchFunc match_func)
      : BaseMap(kDefaultInitialCapacity,
                MatchHashThenKey<void *, MatchFunc>(match_func)) {}
};

}  // namespace tpl::util
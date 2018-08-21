#include "gtest/gtest.h"

#include "util/string_map.h"

namespace tpl::util::test {

TEST(StringMapTest, InsertTest) {
  StringMap<char, std::allocator<char>> map;

  map.insert({"key_1", 'a'});
  map.insert({"key_2", 'b'});

  EXPECT_EQ(2u, map.num_elems());
  EXPECT_EQ(0u, map.num_tombstones());

  {
    auto iter = map.find("key_1");
    EXPECT_NE(iter, map.end());
    EXPECT_EQ(0, std::strcmp("key_1", iter->key()));
    EXPECT_EQ('a', iter->value());
  }

  {
    auto iter = map.find("key_2");
    EXPECT_NE(map.end(), iter);
    EXPECT_EQ(0, std::strcmp("key_2", iter->key()));
    EXPECT_EQ('b', iter->value());
  }
}

TEST(StringMapTest, DeleteTest) {
  StringMap<char, std::allocator<char>> map;

  map.insert({"key_1", 'a'});
  map.insert({"key_2", 'b'});

  EXPECT_EQ(2u, map.num_elems());
  EXPECT_EQ(0u, map.num_tombstones());

  map.erase("key_1");

  EXPECT_EQ(1u, map.num_elems());
  EXPECT_EQ(1u, map.num_tombstones());

  {
    auto iter = map.find("key_1");
    EXPECT_EQ(map.end(), iter);
  }

  // Try reinsertion
  map.insert({"key_1", 'c'});

  EXPECT_EQ(2u, map.num_elems());
  EXPECT_EQ(0u, map.num_tombstones());

  {
    auto iter = map.find("key_1");
    EXPECT_NE(map.end(), iter);
    EXPECT_EQ(0, std::strcmp("key_1", iter->key()));
    EXPECT_EQ('c', iter->value());
  }
}

TEST(StringMapTest, GrowthTest) {
  StringMap<char, std::allocator<char>> map(8);

  uint32_t last_cap = 0;
  for (uint32_t i = 0; i < 32; i++) {
    std::string key = "key_" + std::to_string(i);
    map.insert({key, 'a' + i});

    EXPECT_EQ(i + 1, map.num_elems());
    EXPECT_EQ(0u, map.num_tombstones());
    EXPECT_LE(last_cap, map.capacity());
    last_cap = map.capacity();
  }
}

TEST(StringMapTest, ShrinkTest) {
  StringMap<char, std::allocator<char>> map;

  // Insert stuff
  for (uint32_t i = 0; i < 128; i++) {
    std::string key = "key_" + std::to_string(i);
    map.insert({key, 'a' + i});
  }

  // Delete stuff, ensure capacity never goes up
  uint32_t last_cap = 0;
  for (uint32_t i = 0; i < 256; i++) {
    std::string key = "key_" + std::to_string(i);
    map.erase(key);
    EXPECT_LE(last_cap, map.capacity());
    last_cap = map.capacity();
  }
}

TEST(StringMapTest, CyclicInsertDeleteTest) {
  StringMap<char, std::allocator<char>> map;

  for (uint32_t cycles = 0; cycles < 5; cycles++) {
    // Insert stuff
    for (uint32_t i = 0; i < 128; i++) {
      std::string key = "key_" + std::to_string(i);
      map.insert({key, 'a' + i});
    }

    for (uint32_t i = 0; i < 128; i++) {
      std::string key = "key_" + std::to_string(i);
      EXPECT_NE(map.end(), map.find(key));
    }

    // Delete stuff, ensure capacity never goes up
    for (uint32_t i = 0; i < 256; i++) {
      std::string key = "key_" + std::to_string(i);
      map.erase(key);
    }

    for (uint32_t i = 0; i < 256; i++) {
      std::string key = "key_" + std::to_string(i);
      EXPECT_EQ(map.end(), map.find(key));
    }
  }
}

}  // namespace tpl::util::test

#include <random>
#include <string>
#include <utility>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "util/hash.h"

namespace tpl::util {

class HashTest : public TplTest {};

// Check an input value using a given hashing method
#define CHECK_HASH_METHOD_ON_INPUT(METHOD, INPUT)       \
  {                                                     \
    auto hash_val1 = Hasher::Hash<METHOD>(INPUT);       \
    auto hash_val2 = Hasher::Hash<METHOD>(INPUT);       \
    EXPECT_EQ(hash_val1, hash_val2);                    \
  }                                                     \
  {                                                     \
    auto seed = random_seed();                          \
    auto hash_val1 = Hasher::Hash<METHOD>(INPUT, seed); \
    auto hash_val2 = Hasher::Hash<METHOD>(INPUT, seed); \
    EXPECT_EQ(hash_val1, hash_val2);                    \
  }

// Check an input value against all possible hashing methods
#define CHECK_HASH_ON_INPUT(INPUT)                          \
  {                                                         \
    CHECK_HASH_METHOD_ON_INPUT(HashMethod::Fnv1, INPUT);    \
    CHECK_HASH_METHOD_ON_INPUT(HashMethod::Crc, INPUT);     \
    CHECK_HASH_METHOD_ON_INPUT(HashMethod::Murmur2, INPUT); \
    CHECK_HASH_METHOD_ON_INPUT(HashMethod::xxHash3, INPUT); \
  }

TEST_F(HashTest, IntegerHash) {
  std::random_device random_seed;

  CHECK_HASH_ON_INPUT(i8{-1});
  CHECK_HASH_ON_INPUT(i16{-22});
  CHECK_HASH_ON_INPUT(i32{-333});
  CHECK_HASH_ON_INPUT(i64{-4444});
  CHECK_HASH_ON_INPUT(u8{1});
  CHECK_HASH_ON_INPUT(u16{22});
  CHECK_HASH_ON_INPUT(u32{333});
  CHECK_HASH_ON_INPUT(u64{444});
}

TEST_F(HashTest, FloatingPointHash) {
  std::random_device random_seed;

  CHECK_HASH_ON_INPUT(f32{0});
  CHECK_HASH_ON_INPUT(f32{-213.89});
  CHECK_HASH_ON_INPUT(f64{0});
  CHECK_HASH_ON_INPUT(f64{230984.234});
  CHECK_HASH_ON_INPUT(f64{-230984.234});
}

#undef CHECK_HASH_ON_INPUT
#undef CHECK_HASH_METHOD_ON_INPUT

TEST_F(HashTest, StringHash) {
  const auto small_input = "This is a kinda long string";
  auto large_input = std::string();
  for (u32 i = 0; i < 40; i++) {
    large_input += small_input;
  }

#define CHECK_HASH_METHOD_ON_INPUT(METHOD, INPUT) \
  {                                               \
    auto hash_val1 = Hasher::Hash<METHOD>(INPUT); \
    auto hash_val2 = Hasher::Hash<METHOD>(INPUT); \
    EXPECT_EQ(hash_val1, hash_val2);              \
  }

  // Check an input value against all possible hashing methods
#define CHECK_HASH_ON_INPUT(INPUT)                          \
  {                                                         \
    CHECK_HASH_METHOD_ON_INPUT(HashMethod::Fnv1, INPUT);    \
    CHECK_HASH_METHOD_ON_INPUT(HashMethod::Crc, INPUT);     \
    CHECK_HASH_METHOD_ON_INPUT(HashMethod::Murmur2, INPUT); \
    CHECK_HASH_METHOD_ON_INPUT(HashMethod::xxHash3, INPUT); \
  }

  CHECK_HASH_ON_INPUT("Fixed input");
  CHECK_HASH_ON_INPUT(small_input);
  CHECK_HASH_ON_INPUT(large_input);

#undef CHECK_HASH_ON_INPUT
#undef CHECK_HASH_METHOD_ON_INPUT
}

}  // namespace tpl::util

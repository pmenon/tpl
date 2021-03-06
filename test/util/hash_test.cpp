#include <random>
#include <string>
#include <utility>
#include <vector>

#include "util/hash_util.h"
#include "util/test_harness.h"

namespace tpl::util {

class HashTest : public TplTest {};

// Check an input value using a given hashing method
#define CHECK_HASH_METHOD_ON_INPUT(METHOD, INPUT)   \
  {                                                 \
    auto hash_val1 = HashUtil::METHOD(INPUT);       \
    auto hash_val2 = HashUtil::METHOD(INPUT);       \
    EXPECT_EQ(hash_val1, hash_val2);                \
  }                                                 \
  {                                                 \
    auto seed = RandomDevice()();                   \
    auto hash_val1 = HashUtil::METHOD(INPUT, seed); \
    auto hash_val2 = HashUtil::METHOD(INPUT, seed); \
    EXPECT_EQ(hash_val1, hash_val2);                \
  }

// Check an input value against all possible hashing methods
#define CHECK_HASH_ON_INPUT(INPUT)                 \
  {                                                \
    CHECK_HASH_METHOD_ON_INPUT(HashCrc, INPUT);    \
    CHECK_HASH_METHOD_ON_INPUT(HashMurmur, INPUT); \
    CHECK_HASH_METHOD_ON_INPUT(HashXXH3, INPUT);   \
  }

TEST_F(HashTest, IntegerHash) {
  CHECK_HASH_ON_INPUT(int8_t{-1});
  CHECK_HASH_ON_INPUT(int16_t{-22});
  CHECK_HASH_ON_INPUT(int32_t{-333});
  CHECK_HASH_ON_INPUT(int64_t{-4444});
  CHECK_HASH_ON_INPUT(uint8_t{1});
  CHECK_HASH_ON_INPUT(uint16_t{22});
  CHECK_HASH_ON_INPUT(uint32_t{333});
  CHECK_HASH_ON_INPUT(uint64_t{444});
}

TEST_F(HashTest, FloatingPointHash) {
  CHECK_HASH_ON_INPUT(float{0});
  CHECK_HASH_ON_INPUT(float{-213.89});
  CHECK_HASH_ON_INPUT(double{0});
  CHECK_HASH_ON_INPUT(double{230984.234});
  CHECK_HASH_ON_INPUT(double{-230984.234});
}

#undef CHECK_HASH_ON_INPUT
#undef CHECK_HASH_METHOD_ON_INPUT

TEST_F(HashTest, StringHash) {
  const auto small_input = "This is a kinda long string";
  auto large_input = std::string();
  for (uint32_t i = 0; i < 40; i++) {
    large_input += small_input;
  }

#define CHECK_HASH_ON_INPUT(INPUT)              \
  {                                             \
    auto hash_val1 = HashUtil::HashXXH3(INPUT); \
    auto hash_val2 = HashUtil::HashXXH3(INPUT); \
    EXPECT_EQ(hash_val1, hash_val2);            \
  }

  CHECK_HASH_ON_INPUT("Fixed input");
  CHECK_HASH_ON_INPUT(small_input);
  CHECK_HASH_ON_INPUT(large_input);

#undef CHECK_HASH_ON_INPUT
}

}  // namespace tpl::util

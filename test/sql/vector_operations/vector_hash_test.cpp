#include <random>

#include "sql/vector.h"
#include "sql/vector_operations/vector_operators.h"
#include "util/hash_util.h"
#include "util/sql_test_harness.h"
#include "util/test_harness.h"

namespace tpl::sql {

class VectorHashTest : public TplTest {};

TEST_F(VectorHashTest, NumericHashes) {
#define GEN_HASH_TEST(TYPE_ID, CPP_TYPE)                                                  \
  {                                                                                       \
    auto input = Make##TYPE_ID##Vector(257);                                              \
    auto hashes = MakeVector(TypeId::Hash, 257);                                          \
    /* Fill input */                                                                      \
    std::random_device r;                                                                 \
    for (uint64_t i = 0; i < input->GetSize(); i++) {                                     \
      input->SetValue(i, GenericValue::Create##TYPE_ID(r()));                             \
    }                                                                                     \
    /* Hash */                                                                            \
    VectorOps::Hash(*input, hashes.get());                                                \
    EXPECT_EQ(input->GetSize(), hashes->GetSize());                                       \
    EXPECT_EQ(input->GetCount(), hashes->GetCount());                                     \
    EXPECT_EQ(nullptr, hashes->GetSelectionVector());                                     \
    /* Check output */                                                                    \
    VectorOps::Exec(*input, [&](uint64_t i, uint64_t k) {                                 \
      EXPECT_EQ(reinterpret_cast<hash_t *>(hashes->GetData())[i],                         \
                util::HashUtil::Hash(reinterpret_cast<CPP_TYPE *>(input->GetData())[i])); \
    });                                                                                   \
  }

  GEN_HASH_TEST(TinyInt, int8_t);
  GEN_HASH_TEST(SmallInt, int16_t);
  GEN_HASH_TEST(Integer, int32_t);
  GEN_HASH_TEST(BigInt, int64_t);
  GEN_HASH_TEST(Float, float);
  GEN_HASH_TEST(Double, double);

#undef GEN_HASH_TEST
}

TEST_F(VectorHashTest, HashWithNullInput) {
  // input = [1.2, 3.45, NULL, NULL, NULL]
  auto input =
      MakeFloatVector({1.2, 3.45, 67.89, 123.456, 789.01}, {false, false, true, true, true});
  auto hash = MakeVector(TypeId::Hash, input->GetSize());

  VectorOps::Hash(*input, hash.get());

  EXPECT_EQ(input->GetSize(), hash->GetSize());
  EXPECT_EQ(input->GetCount(), hash->GetCount());
  EXPECT_EQ(nullptr, hash->GetSelectionVector());

  auto raw_input = reinterpret_cast<float *>(input->GetData());
  auto raw_hash = reinterpret_cast<hash_t *>(hash->GetData());
  EXPECT_EQ(util::HashUtil::Hash(raw_input[0]), raw_hash[0]);
  EXPECT_EQ(util::HashUtil::Hash(raw_input[1]), raw_hash[1]);
  EXPECT_EQ(0, raw_hash[2]);
  EXPECT_EQ(0, raw_hash[3]);
  EXPECT_EQ(0, raw_hash[4]);
}

TEST_F(VectorHashTest, StringHash) {
  // input = [1.2, 3.45, NULL, NULL, NULL]
  const char *refs[] = {
      "short", "medium sized", "quite long indeed, but why, so?",
      "I'm trying to right my wrongs, but it's funny, them same wrongs help me write this song"};
  auto input = MakeVarcharVector({refs[0], refs[1], refs[2], refs[3]}, {false, true, false, false});
  auto hash = MakeVector(TypeId::Hash, input->GetSize());

  VectorOps::Hash(*input, hash.get());

  EXPECT_EQ(input->GetSize(), hash->GetSize());
  EXPECT_EQ(input->GetCount(), hash->GetCount());
  EXPECT_EQ(nullptr, hash->GetSelectionVector());

  auto raw_input = reinterpret_cast<const VarlenEntry *>(input->GetData());
  auto raw_hash = reinterpret_cast<hash_t *>(hash->GetData());
  EXPECT_EQ(raw_input[0].Hash(), raw_hash[0]);
  EXPECT_EQ(hash_t(0), raw_hash[1]);
  EXPECT_EQ(raw_input[2].Hash(), raw_hash[2]);
  EXPECT_EQ(raw_input[3].Hash(), raw_hash[3]);
}

}  // namespace tpl::sql

#include "sql/compact_storage.h"
#include "util/test_harness.h"

namespace tpl::sql {

class CompactStorageTest : public TplTest {};

TEST_F(CompactStorageTest, BasicTest) {
  // Schema: int,tiny,bigint,small,varchar
  // Reordered: varchar,bigint,int,small,tiny,NULL bitmap (1 byte)
  CompactStorage storage(
      {TypeId::Integer, TypeId::TinyInt, TypeId::BigInt, TypeId::SmallInt, TypeId::Varchar});
  EXPECT_EQ(5u, storage.GetNumElements());
  EXPECT_EQ(16u + 8u + 4u + 2u + 1u + 1u, storage.GetRequiredSize());

  // The temporary buffer.
  std::unique_ptr<byte[]> buffer = std::make_unique<byte[]>(storage.GetRequiredSize());

  // Try some writes.
  // Remember, we write in the order we want. Storage handles reordering.
  {
    int32_t v;
    bool null = false;
    storage.Write<int32_t, false>(0, buffer.get(), 10, null);
    storage.Read<int32_t, false>(0, buffer.get(), &v, &null);
    EXPECT_EQ(10, v);
    EXPECT_FALSE(null);
  }

  {
    // Try a non-NULL tinyint.
    int8_t v;
    bool null = false;
    storage.Write<int8_t, true>(1, buffer.get(), 101, null);
    storage.Read<int8_t, true>(1, buffer.get(), &v, &null);
    EXPECT_EQ(101, v);
    EXPECT_FALSE(null);

    // Now, try a NULL tinyint.
    null = true;
    storage.Write<int8_t, true>(1, buffer.get(), 44, null);
    storage.Read<int8_t, true>(1, buffer.get(), &v, &null);
    EXPECT_TRUE(null);
  }

  {
    // Try some strings.
    VarlenEntry e, in = VarlenEntry::Create("ICAN'TLOSE");
    bool null = false;
    storage.Write<VarlenEntry, false>(4, buffer.get(), in, null);
    storage.Read<VarlenEntry, false>(4, buffer.get(), &e, &null);
    EXPECT_EQ(in, e);
    EXPECT_FALSE(null);

    // Now NULL strings.
    e = VarlenEntry::Create("BRUH");
    storage.Write<VarlenEntry, true>(4, buffer.get(), in, true);
    storage.Read<VarlenEntry, true>(4, buffer.get(), &e, &null);
    EXPECT_TRUE(null);

    // Validate that the tiny int we wrote earlier is still NULL.
    null = false;
    int8_t tiny;
    storage.Read<int8_t, true>(1, buffer.get(), &tiny, &null);
    EXPECT_TRUE(null);
  }
}

}  // namespace tpl::sql

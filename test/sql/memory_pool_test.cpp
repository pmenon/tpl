#include <utility>

#include "tpl_test.h"  // NOLINT

#include "sql/memory_pool.h"

namespace tpl::sql {

class MemoryPoolTest : public TplTest {};

struct SimpleObj {
  u32 a, b, c, d;
};

struct ComplexObj {
  MemoryPool *memory;
  MemPoolPtr<SimpleObj> nested;
  ComplexObj(MemoryPool *m, MemPoolPtr<SimpleObj> n)
      : memory(m), nested(std::move(n)) {}
  ~ComplexObj() { memory->FreeObject(std::move(nested)); }
};

TEST_F(MemoryPoolTest, PoolPointers) {
  MemoryPool pool(nullptr);

  // Empty pointer
  MemPoolPtr<SimpleObj> obj1, obj2;
  EXPECT_EQ(obj1, obj2);
  EXPECT_EQ(nullptr, obj1);
  EXPECT_EQ(obj1, nullptr);
  EXPECT_EQ(nullptr, obj1.get());

  // Non-empty pointers
  obj1 = pool.NewObject<SimpleObj>();
  EXPECT_NE(obj1, obj2);
  EXPECT_NE(nullptr, obj1);
  EXPECT_NE(obj1, nullptr);
  EXPECT_NE(nullptr, obj1.get());

  obj1->a = 10;
  EXPECT_EQ(10u, obj1->a);

  pool.FreeObject(std::move(obj1));
}

TEST_F(MemoryPoolTest, ComplexPointers) {
  MemoryPool pool(nullptr);

  MemPoolPtr<ComplexObj> obj1, obj2;
  EXPECT_EQ(obj1, obj2);

  obj1 = pool.NewObject<ComplexObj>(&pool, pool.NewObject<SimpleObj>());
  EXPECT_NE(nullptr, obj1);
  EXPECT_NE(obj1, nullptr);

  obj1->nested->a = 1;
  EXPECT_EQ(1u, obj1->nested->a);

  pool.FreeObject(std::move(obj1));
}

}  // namespace tpl::sql

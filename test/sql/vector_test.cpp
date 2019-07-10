#include <limits>
#include <numeric>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "sql/vector.h"
#include "util/bit_util.h"

namespace tpl::sql::test {

class VectorTest : public TplTest {};

TEST_F(VectorTest, CheckEmpty) {
  // Creating an empty vector should have zero count and no selection vector
  Vector vec1(TypeId::Integer);
  EXPECT_EQ(0u, vec1.count());
  EXPECT_EQ(nullptr, vec1.selection_vector());

  Vector vec2(TypeId::Integer, false, true);
  EXPECT_EQ(0u, vec2.count());
  EXPECT_EQ(nullptr, vec2.selection_vector());

  // Vectors that allocate must clean up
  Vector vec3(TypeId::Boolean, true, false);
}

TEST_F(VectorTest, Clear) {
  // Allocate and clear vector
  Vector vec(TypeId::TinyInt, true, true);
  vec.set_count(10);

  // All elements should 0
  for (u32 i = 0; i < 10; i++) {
    auto val = vec.GetValue(i);
    EXPECT_EQ(GenericValue::CreateTinyInt(0), val);
  }
}

TEST_F(VectorTest, InitFromArray) {
  const u32 num_elems = 5;

  // Try simple arithmetic vector
  {
    f32 arr[num_elems] = {-1.2, -34.56, 6.7, 8.91011, 1213.1415};

    Vector vec(TypeId::Float, reinterpret_cast<byte *>(arr), num_elems);
    EXPECT_EQ(num_elems, vec.count());
    EXPECT_EQ(nullptr, vec.selection_vector());

    for (u32 i = 0; i < num_elems; i++) {
      auto val = vec.GetValue(i);
      EXPECT_EQ(GenericValue::CreateReal(arr[i]), val);
    }
  }

  // Now a string array
  {
    const char *arr[num_elems] = {"go loko", "hot-line bling", "kawhi", "6ix",
                                  "king city"};
    Vector vec(TypeId::Varchar, reinterpret_cast<byte *>(arr), num_elems);
    EXPECT_EQ(num_elems, vec.count());
    EXPECT_EQ(nullptr, vec.selection_vector());

    for (u32 i = 0; i < num_elems; i++) {
      auto val = vec.GetValue(i);
      EXPECT_EQ(GenericValue::CreateString(arr[i]), val);
    }
  }
}

TEST_F(VectorTest, GetAndSet) {
  Vector vec(TypeId::Boolean, true, false);
  vec.set_count(10);

  // vec[0] = false
  vec.SetValue(0, GenericValue::CreateBoolean(false));
  EXPECT_EQ(GenericValue::CreateBoolean(false), vec.GetValue(0));

  // vec[0] = true (NULL)
  vec.SetNull(0, true);
  EXPECT_TRUE(vec.GetValue(0).is_null());

  // vec[0] = true
  vec.SetValue(0, GenericValue::CreateBoolean(true));
  EXPECT_EQ(GenericValue::CreateBoolean(true), vec.GetValue(0));
}

TEST_F(VectorTest, GetAndSetString) {
  Vector vec(TypeId::Varchar, true, false);
  vec.set_count(10);

  vec.SetValue(0, GenericValue::CreateString("hello"));
  EXPECT_EQ(GenericValue::CreateString("hello"), vec.GetValue(0));

  vec.SetNull(0, true);
  EXPECT_TRUE(vec.GetValue(0).is_null());
}

TEST_F(VectorTest, Reference) {
  Vector vec(TypeId::Integer, true, true);
  vec.set_count(10);
  for (u32 i = 0; i < 10; i++) {
    vec.SetValue(i, GenericValue::CreateInteger(i));
  }

  // Create a new vector that references the one we just created. We
  // intentionally create it with a different type to ensure we switch types.
  {
    Vector vec2(TypeId::Boolean);
    vec2.Reference(&vec);
    EXPECT_TRUE(vec2.type_id() == TypeId::Integer);
    EXPECT_EQ(vec.count(), vec2.count());
    for (u32 i = 0; i < vec.count(); i++) {
      EXPECT_FALSE(vec2.IsNull(i));
      EXPECT_EQ(vec2.GetValue(i), vec2.GetValue(i));
    }
  }
}

TEST_F(VectorTest, Move) {
  // First try to reference a backing STL vector
  std::vector<sel_t> sel = {0, 2, 4, 6, 8};

  Vector vec(TypeId::Integer, true, true);
  vec.set_count(10);
  for (u32 i = 0; i < 10; i++) {
    vec.SetValue(i, GenericValue::CreateInteger(i));
  }
  vec.SetSelectionVector(sel.data(), sel.size());

  // Move the original vector to the target
  Vector target(vec.type_id());
  vec.MoveTo(&target);

  // First, the old vector should empty
  EXPECT_EQ(0u, vec.count());
  EXPECT_EQ(nullptr, vec.selection_vector());
  EXPECT_EQ(nullptr, vec.data());

  // The new vector should own the data
  EXPECT_EQ(sel.size(), target.count());
  EXPECT_NE(nullptr, target.selection_vector());
  EXPECT_NE(nullptr, target.data());

  // The original vector was [0,9], the selection vector selects the even
  // elements [0,2,4,6,8]
  for (u32 i = 0; i < target.count(); i++) {
    EXPECT_FALSE(vec.IsNull(i));
    EXPECT_EQ(GenericValue::CreateInteger(sel[i]), target.GetValue(i));
  }
}

TEST_F(VectorTest, Copy) {
  // First try to reference a backing STL vector
  std::vector<sel_t> sel = {0, 2, 4, 6, 8};

  Vector vec(TypeId::Integer, true, true);
  vec.set_count(10);
  for (u32 i = 0; i < 10; i++) {
    vec.SetValue(i, GenericValue::CreateInteger(i));
  }
  vec.SetSelectionVector(sel.data(), sel.size());

  // Move the original vector to the target
  Vector target(vec.type_id(), true, true);
  vec.CopyTo(&target);

  // Expect same count, but no selection vector
  EXPECT_EQ(sel.size(), target.count());
  EXPECT_EQ(nullptr, target.selection_vector());

  for (u32 i = 0; i < target.count(); i++) {
    EXPECT_EQ(vec.GetValue(i), target.GetValue(i));
  }
}

TEST_F(VectorTest, CopyWithOffset) {
  std::vector<sel_t> sel = {0, 2, 4, 6, 8};

  Vector vec(TypeId::Integer, true, true);
  vec.set_count(10);
  for (u32 i = 0; i < 10; i++) {
    vec.SetValue(i, GenericValue::CreateInteger(i));
  }
  vec.SetSelectionVector(sel.data(), sel.size());

  const u32 offset = 2;

  // Move the original vector to the target
  Vector target(vec.type_id(), true, true);
  vec.CopyTo(&target, offset);

  // Expect same count, but no selection vector
  EXPECT_EQ(sel.size() - offset, target.count());
  EXPECT_EQ(nullptr, target.selection_vector());

  for (u32 i = 0; i < target.count(); i++) {
    EXPECT_EQ(vec.GetValue(i + offset), target.GetValue(i));
  }
}

TEST_F(VectorTest, CopyStringVector) {
  Vector vec(TypeId::Varchar, true, true);
  vec.set_count(10);
  for (u32 i = 0; i < 10; i++) {
    vec.SetValue(i, GenericValue::CreateString("val-" + std::to_string(i)));
  }

  // Filter the even elements
  std::vector<sel_t> sel = {0, 2, 4, 6, 8};
  vec.SetSelectionVector(sel.data(), sel.size());
  vec.SetNull(1, true);

  Vector target(TypeId::Varchar, true, true);
  vec.CopyTo(&target);

  for (u32 i = 0; i < target.count(); i++) {
    auto src_val_ptr = vec.GetValue(i);
    auto target_val_ptr = target.GetValue(i);
    EXPECT_EQ(vec.GetValue(i), target.GetValue(i));
  }
}

TEST_F(VectorTest, Cast) {
  // First try to reference a backing STL vector
  std::vector<i32> base_stdvec = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  std::vector<sel_t> sel = {0, 2, 4, 6, 8};

  // First, try happy-path upcast from i32 -> i64
  {
    Vector vec(TypeId::Integer, true, true);
    vec.set_count(10);
    for (u32 i = 0; i < 10; i++) {
      vec.SetValue(i, GenericValue::CreateInteger(i));
    }
    vec.SetSelectionVector(sel.data(), sel.size());

    EXPECT_NO_THROW(vec.Cast(TypeId::BigInt));
    EXPECT_TRUE(vec.type_id() == TypeId::BigInt);
    EXPECT_EQ(sel.size(), vec.count());
    EXPECT_NE(nullptr, vec.selection_vector());

    for (u32 i = 0; i < vec.count(); i++) {
      EXPECT_EQ(GenericValue::CreateBigInt(base_stdvec[sel[i]]),
                vec.GetValue(i));
    }
  }

  // Second happy path, try i32 -> i16 with valid i16 values
  {
    Vector vec(TypeId::Integer, true, true);
    vec.set_count(10);
    for (u32 i = 0; i < 10; i++) {
      vec.SetValue(i, GenericValue::CreateInteger(i));
    }
    vec.SetSelectionVector(sel.data(), sel.size());

    EXPECT_NO_THROW(vec.Cast(TypeId::SmallInt));
    EXPECT_TRUE(vec.type_id() == TypeId::SmallInt);
    EXPECT_EQ(sel.size(), vec.count());
    EXPECT_NE(nullptr, vec.selection_vector());

    for (u32 i = 0; i < vec.count(); i++) {
      EXPECT_EQ(GenericValue::CreateSmallInt(base_stdvec[sel[i]]),
                vec.GetValue(i));
    }
  }

  // Third, try i32 -> i16 again, but make one of the valid values out of range
  {
    Vector vec(TypeId::Integer, true, true);
    vec.set_count(10);
    for (u32 i = 0; i < 10; i++) {
      vec.SetValue(i, GenericValue::CreateInteger(i));
    }
    vec.SetSelectionVector(sel.data(), sel.size());

    vec.SetValue(
        1, GenericValue::CreateInteger(std::numeric_limits<i16>::max() + 44));

    EXPECT_THROW(vec.Cast(TypeId::SmallInt), std::runtime_error);
  }
}

TEST_F(VectorTest, Append) {
  Vector vec1(TypeId::Double, true, true);
  vec1.set_count(3);
  vec1.SetValue(0, GenericValue::CreateDouble(1.0));
  vec1.SetValue(1, GenericValue::CreateDouble(2.0));
  vec1.SetValue(2, GenericValue::CreateDouble(3.0));

  Vector vec2(TypeId::Double, true, false);
  vec2.set_count(2);
  vec2.SetValue(0, GenericValue::CreateDouble(10.0));
  vec2.SetValue(1, GenericValue::CreateDouble(11.0));

  vec2.Append(vec1);

  EXPECT_EQ(5u, vec2.count());
  EXPECT_EQ(nullptr, vec2.selection_vector());

  EXPECT_EQ(GenericValue::CreateDouble(10.0), vec2.GetValue(0));
  EXPECT_EQ(GenericValue::CreateDouble(11.0), vec2.GetValue(1));
  EXPECT_EQ(GenericValue::CreateDouble(1.0), vec2.GetValue(2));
  EXPECT_EQ(GenericValue::CreateDouble(2.0), vec2.GetValue(3));
  EXPECT_EQ(GenericValue::CreateDouble(3.0), vec2.GetValue(4));
}

TEST_F(VectorTest, AppendWithSelectionVector) {
  std::vector<sel_t> sel1 = {1};
  Vector vec1(TypeId::Double, true, true);
  vec1.set_count(3);
  vec1.SetValue(0, GenericValue::CreateDouble(1.0));
  vec1.SetValue(1, GenericValue::CreateDouble(2.0));
  vec1.SetValue(2, GenericValue::CreateDouble(3.0));
  vec1.SetSelectionVector(sel1.data(), sel1.size());

  Vector vec2(TypeId::Double, true, false);
  vec2.set_count(2);
  vec2.SetValue(0, GenericValue::CreateDouble(10.0));
  vec2.SetValue(1, GenericValue::CreateDouble(11.0));

  vec2.Append(vec1);

  EXPECT_EQ(3u, vec2.count());
  EXPECT_EQ(nullptr, vec2.selection_vector());

  EXPECT_EQ(GenericValue::CreateDouble(10.0), vec2.GetValue(0));
  EXPECT_EQ(GenericValue::CreateDouble(11.0), vec2.GetValue(1));
  EXPECT_EQ(GenericValue::CreateDouble(2.0), vec2.GetValue(2));
}

TEST_F(VectorTest, Print) {
  {
    Vector vec(TypeId::Boolean, true, true);
    vec.set_count(4);
    vec.SetValue(0, GenericValue::CreateBoolean(false));
    vec.SetValue(1, GenericValue::CreateBoolean(true));
    vec.SetValue(2, GenericValue::CreateBoolean(true));
    vec.SetValue(3, GenericValue::CreateBoolean(false));
    EXPECT_EQ("False,True,True,False", vec.ToString());
  }

#define CHECK_NUMERIC_VECTOR_PRINT(TYPE)             \
  {                                                  \
    Vector vec(TypeId::TYPE, true, true);            \
    vec.set_count(4);                                \
    vec.SetValue(0, GenericValue::Create##TYPE(10)); \
    vec.SetValue(1, GenericValue::Create##TYPE(20)); \
    vec.SetValue(2, GenericValue::Create##TYPE(30)); \
    vec.SetValue(3, GenericValue::Create##TYPE(40)); \
    vec.SetNull(1, true);                            \
    EXPECT_EQ("10,NULL,30,40", vec.ToString());      \
  };

  CHECK_NUMERIC_VECTOR_PRINT(TinyInt);
  CHECK_NUMERIC_VECTOR_PRINT(SmallInt);
  CHECK_NUMERIC_VECTOR_PRINT(Integer);
  CHECK_NUMERIC_VECTOR_PRINT(BigInt);
}

}  // namespace tpl::sql::test

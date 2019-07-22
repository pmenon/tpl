#include <random>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "sql/operations/comparison_operators.h"
#include "sql/sorter.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"

namespace tpl::sql::test {

namespace {

/**
 * The input tuples. All sorter instances use this tuple as input and output.
 */
struct Tuple {
  i64 key;
  i64 attributes;
};

/**
 * Insert a number of input tuples into the given sorter instance.
 * @param sorter The sorter to insert into.
 * @param num_tuples The number of tuple to insert.
 */
void PopulateSorter(Sorter *sorter, u32 num_tuples = kDefaultVectorSize + 7) {
  std::random_device r;
  for (u32 i = 0; i < num_tuples; i++) {
    auto tuple = reinterpret_cast<Tuple *>(sorter->AllocInputTuple());
    tuple->key = r() % num_tuples;
    tuple->attributes = r() % 10;
  }
}

/**
 * Compare two tuples.
 * @param lhs The first tuple.
 * @param rhs The second tuple.
 * @return < 0 if lhs < rhs, 0 if lhs = rhs, > 0 if lhs > rhs.
 */
i32 CompareTuple(const Tuple &lhs, const Tuple &rhs) {
  return lhs.key - rhs.key;
}

/**
 * Convert row-oriented data in the rows vector to columnar format in the given
 * vector projection.
 * @param rows The array of row-oriented tuple data.
 * @param num_rows The number of rows in the array.
 * @param iter The output vector projection iterator.
 */
void Transpose(const byte *rows[], u64 num_rows,
               VectorProjectionIterator *iter) {
  for (u32 i = 0; i < num_rows; i++) {
    auto tuple = reinterpret_cast<const Tuple *>(rows[i]);
    iter->SetValue<i64, false>(0, tuple->key, false);
    iter->SetValue<i64, false>(1, tuple->attributes, false);
    iter->Advance();
  }
}

/**
 * Check if the given vector is sorted.
 * @tparam T The primitive/native type of the element the vector contains.
 * @param vec The vector to check is sorted.
 * @return True if sorted; false otherwise.
 */
template <class T>
bool IsSorted(const Vector &vec) {
  const auto data = reinterpret_cast<const T *>(vec.data());
  for (u32 i = 1; i < vec.count(); i++) {
    bool left_null = vec.null_mask()[i];
    bool right_null = vec.null_mask()[i];
    if (!left_null && right_null) {
      return false;
    } else if (left_null) {
      continue;
    } else {
      if (!LessThanEqual::Apply<T>(data[i], data[i])) {
        return false;
      }
    }
  }
  return true;
}

}  // namespace

class SorterVectorIteratorTest : public TplTest {
 public:
  SorterVectorIteratorTest()
      : memory_(nullptr),
        schema_({{"key", BigIntType::InstanceNonNullable()},
                 {"attributes", BigIntType::InstanceNullable()}}) {}

 protected:
  MemoryPool *memory() { return &memory_; }

  std::vector<const Schema::ColumnInfo *> row_meta() const {
    std::vector<const Schema::ColumnInfo *> cols;
    for (const auto &col : schema_.columns()) {
      cols.push_back(&col);
    }
    return cols;
  }

 private:
  MemoryPool memory_;
  Schema schema_;
};

TEST_F(SorterVectorIteratorTest, EmptyIterator) {
  const auto compare = [](const void *lhs, const void *rhs) {
    return CompareTuple(*reinterpret_cast<const Tuple *>(lhs),
                        *reinterpret_cast<const Tuple *>(rhs));
  };
  Sorter sorter(memory(), compare, sizeof(Tuple));

  for (SorterVectorIterator iter(sorter, row_meta(), Transpose); iter.HasNext();
       iter.Next(Transpose)) {
    FAIL() << "Iteration should not occur on empty sorter instance";
  }
}

TEST_F(SorterVectorIteratorTest, Iterate) {
  const u32 num_elems = kDefaultVectorSize + 29;
  const auto compare = [](const void *lhs, const void *rhs) {
    return CompareTuple(*reinterpret_cast<const Tuple *>(lhs),
                        *reinterpret_cast<const Tuple *>(rhs));
  };
  Sorter sorter(memory(), compare, sizeof(Tuple));
  PopulateSorter(&sorter, num_elems);
  sorter.Sort();

  u32 num_found = 0;
  for (SorterVectorIterator iter(sorter, row_meta(), Transpose); iter.HasNext();
       iter.Next(Transpose)) {
    auto *vpi = iter.GetVectorProjectionIterator();
    // Count
    num_found += vpi->num_selected();
    // Verify sorted
    IsSorted<i64>(*vpi->GetVectorProjection()->GetColumn(0));
  }

  EXPECT_EQ(num_elems, num_found);
}

}  // namespace tpl::sql::test

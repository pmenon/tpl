#include <chrono>
#include <vector>

#include "gmock/gmock.h"

#include "common/settings.h"
#include "sql/constant_vector.h"
#include "sql/filter_manager.h"
#include "sql/table_vector_iterator.h"
#include "sql/vector_operations/vector_operations.h"
#include "util/test_harness.h"

namespace tpl::sql {

class FilterManagerTest : public TplTest {};

enum Col : uint8_t { A = 0, B = 1, C = 2, D = 3 };

using namespace std::chrono_literals;  // NOLINT

TEST_F(FilterManagerTest, ConjunctionTest) {
  // Create a filter that implements: colA < 500 AND colB < 9
  FilterManager filter;
  filter.StartNewClause();
  filter.InsertClauseTerms(
      {[](auto vp, auto tids, auto ctx) {
         VectorOps::SelectLessThan(*vp->GetColumn(Col::A),
                                   ConstantVector(GenericValue::CreateInteger(500)), tids);
       },
       [](auto vp, auto tids, auto ctx) {
         VectorOps::SelectLessThan(*vp->GetColumn(Col::B),
                                   ConstantVector(GenericValue::CreateInteger(9)), tids);
       }});

  // Create some random data.
  VectorProjection vp;
  vp.Initialize({TypeId::Integer, TypeId::Integer});
  vp.Reset(kDefaultVectorSize);
  VectorOps::Generate(vp.GetColumn(Col::A), 0, 1);
  VectorOps::Generate(vp.GetColumn(Col::B), 0, 1);

  // Run the filters
  VectorProjectionIterator vpi(&vp);
  filter.RunFilters(&vpi);

  // Check
  EXPECT_TRUE(vpi.IsFiltered());
  EXPECT_FALSE(vpi.IsEmpty());
  vpi.ForEach([&]() {
    auto cola = *vpi.GetValue<int32_t, false>(Col::A, nullptr);
    auto colb = *vpi.GetValue<int32_t, false>(Col::B, nullptr);
    EXPECT_TRUE(cola < 500 && colb < 9);
  });
}

TEST_F(FilterManagerTest, DisjunctionTest) {
  // Create a filter that implements: colA < 500 OR colB < 9
  FilterManager filter;
  filter.StartNewClause();
  filter.InsertClauseTerm([](auto vp, auto tids, auto ctx) {
    VectorOps::SelectLessThan(*vp->GetColumn(Col::A),
                              ConstantVector(GenericValue::CreateInteger(500)), tids);
  });
  filter.StartNewClause();
  filter.InsertClauseTerm([](auto vp, auto tids, auto ctx) {
    VectorOps::SelectLessThan(*vp->GetColumn(Col::B),
                              ConstantVector(GenericValue::CreateInteger(9)), tids);
  });

  // Create some random data.
  VectorProjection vp;
  vp.Initialize({TypeId::Integer, TypeId::Integer});
  vp.Reset(kDefaultVectorSize);
  VectorOps::Generate(vp.GetColumn(Col::A), 0, 1);
  VectorOps::Generate(vp.GetColumn(Col::B), 0, 1);

  // Run the filters
  VectorProjectionIterator vpi(&vp);
  filter.RunFilters(&vpi);

  // Check
  EXPECT_TRUE(vpi.IsFiltered());
  EXPECT_FALSE(vpi.IsEmpty());
  vpi.ForEach([&]() {
    auto cola = *vpi.GetValue<int32_t, false>(Col::A, nullptr);
    auto colb = *vpi.GetValue<int32_t, false>(Col::B, nullptr);
    EXPECT_TRUE(cola < 500 || colb < 9);
  });
}

TEST_F(FilterManagerTest, MixedTaatVaatFilterTest) {
  // Create a filter that implements: colA < 500 AND colB < 9
  // The filter on column colB is implemented using a tuple-at-a-time filter.
  // Thus, the filter is a mixed VaaT and TaaT filter.
  FilterManager filter;
  filter.StartNewClause();
  filter.InsertClauseTerms(
      {[](auto vp, auto tids, auto ctx) {
         VectorOps::SelectLessThan(*vp->GetColumn(Col::A),
                                   ConstantVector(GenericValue::CreateInteger(500)), tids);
       },
       [](auto vp, auto tids, auto ctx) {
         VectorProjectionIterator iter(vp, tids);
         iter.RunFilter([&]() { return *iter.GetValue<int32_t, false>(Col::B, nullptr) < 9; });
       }});

  // Create some random data.
  VectorProjection vp;
  vp.Initialize({TypeId::Integer, TypeId::Integer});
  vp.Reset(kDefaultVectorSize);
  VectorOps::Generate(vp.GetColumn(Col::A), 0, 1);
  VectorOps::Generate(vp.GetColumn(Col::B), 0, 1);

  // Run the filters
  VectorProjectionIterator vpi(&vp);
  filter.RunFilters(&vpi);

  // Check
  EXPECT_TRUE(vpi.IsFiltered());
  EXPECT_FALSE(vpi.IsEmpty());
  vpi.ForEach([&]() {
    auto cola = *vpi.GetValue<int32_t, false>(Col::A, nullptr);
    auto colb = *vpi.GetValue<int32_t, false>(Col::B, nullptr);
    EXPECT_TRUE(cola < 500 && colb < 9);
  });
}

TEST_F(FilterManagerTest, AdaptiveCheckTest) {
  uint32_t iter = 0;

  // Create a filter that implements: colA < 500 AND colB < 9
  FilterManager filter(true, &iter);
  filter.StartNewClause();
  filter.InsertClauseTerms({[](auto vp, auto tids, auto ctx) {
                              auto *r = reinterpret_cast<uint32_t *>(ctx);
                              if (*r < 100) std::this_thread::sleep_for(1us);  // Fake a sleep.
                              const auto val = ConstantVector(GenericValue::CreateInteger(500));
                              VectorOps::SelectLessThan(*vp->GetColumn(Col::A), val, tids);
                            },
                            [](auto vp, auto tids, auto ctx) {
                              auto *r = reinterpret_cast<uint32_t *>(ctx);
                              if (*r > 100) std::this_thread::sleep_for(1us);  // Fake a sleep.
                              const auto val = ConstantVector(GenericValue::CreateInteger(9));
                              VectorOps::SelectLessThan(*vp->GetColumn(Col::B), val, tids);
                            }});

  // Create some random data.
  VectorProjection vp;
  vp.Initialize({TypeId::Integer, TypeId::Integer});
  vp.Reset(kDefaultVectorSize);
  VectorOps::Generate(vp.GetColumn(Col::A), 0, 1);
  VectorOps::Generate(vp.GetColumn(Col::B), 0, 1);

  for (uint32_t run = 0; run < 2; run++) {
    for (uint32_t i = 0; i < 100; i++, iter++) {
      // Remove any lingering filter.
      vp.Reset(kDefaultVectorSize);
      // Create an iterator and filter it.
      VectorProjectionIterator vpi(&vp);
      filter.RunFilters(&vpi);
    }

    // After a while, at least one re sampling should have occurred. At that time,
    // the manager should have realized that the second filter is more selective
    // and runs faster. When we switch to the second run, the second filter is
    // hobbled and the order should reverse back.
    EXPECT_EQ(1, filter.GetClauseCount());
    const auto clause = filter.GetOptimalClauseOrder()[0];
    EXPECT_GT(clause->GetResampleCount(), 1);
    if (run == 0) {
      EXPECT_THAT(clause->GetOptimalTermOrder(), ::testing::ElementsAre(1, 0));
    } else {
      EXPECT_THAT(clause->GetOptimalTermOrder(), ::testing::ElementsAre(0, 1));
    }
  }
}

}  // namespace tpl::sql

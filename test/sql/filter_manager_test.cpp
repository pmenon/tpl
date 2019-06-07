#include <chrono>  // NOLINT
#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "sql_test.h"  // NOLINT

#include "sql/catalog.h"
#include "sql/filter_manager.h"
#include "sql/table_vector_iterator.h"

namespace tpl::sql::test {

class FilterManagerTest : public SqlBasedTest {};

enum Col : u8 { A = 0, B = 1, C = 2, D = 3 };

u32 TaaT_Lt_500(VectorProjectionIterator *vpi) {
  vpi->RunFilter([vpi]() -> bool {
    auto cola = *vpi->GetValue<i32, false>(Col::A, nullptr);
    return cola < 500;
  });
  return vpi->num_selected();
}

u32 Hobbled_TaaT_Lt_500(VectorProjectionIterator *vpi) {
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  return TaaT_Lt_500(vpi);
}

u32 Vectorized_Lt_500(VectorProjectionIterator *vpi) {
  VectorProjectionIterator::FilterVal param{.i = 500};
  return vpi->FilterColByVal<std::less>(Col::A, param);
}

TEST_F(FilterManagerTest, SimpleFilterManagerTest) {
  FilterManager filter(bandit::Policy::FixedAction);
  filter.StartNewClause();
  filter.InsertClauseFlavor(TaaT_Lt_500);
  filter.InsertClauseFlavor(Vectorized_Lt_500);
  filter.Finalize();

  TableVectorIterator tvi(static_cast<u16>(TableId::Test1));
  for (tvi.Init(); tvi.Advance();) {
    auto *vpi = tvi.vector_projection_iterator();

    // Run the filters
    filter.RunFilters(vpi);

    // Check
    vpi->ForEach([vpi]() {
      auto cola = *vpi->GetValue<i32, false>(Col::A, nullptr);
      EXPECT_LT(cola, 500);
    });
  }
}

TEST_F(FilterManagerTest, AdaptiveFilterManagerTest) {
  FilterManager filter(bandit::Policy::EpsilonGreedy);
  filter.StartNewClause();
  filter.InsertClauseFlavor(Hobbled_TaaT_Lt_500);
  filter.InsertClauseFlavor(Vectorized_Lt_500);
  filter.Finalize();

  TableVectorIterator tvi(static_cast<u16>(TableId::Test1));
  for (tvi.Init(); tvi.Advance();) {
    auto *vpi = tvi.vector_projection_iterator();

    // Run the filters
    filter.RunFilters(vpi);

    // Check
    vpi->ForEach([vpi]() {
      auto cola = *vpi->GetValue<i32, false>(Col::A, nullptr);
      EXPECT_LT(cola, 500);
    });
  }

  // The vectorized filter better be the optimal!
  EXPECT_EQ(1u, filter.GetOptimalFlavorForClause(0));
}

}  // namespace tpl::sql::test

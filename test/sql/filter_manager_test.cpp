#include <limits>
#include <memory>
#include <numeric>
#include <random>
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
    auto cola = *vpi->Get<i32, false>(Col::A, nullptr);
    return cola < 500;
  });
  return vpi->num_selected();
}

u32 Vectorized_Lt_500(VectorProjectionIterator *vpi) {
  VectorProjectionIterator::FilterVal param{.i = 500};
  return vpi->FilterColByVal<std::less>(Col::A, param);
}

TEST_F(FilterManagerTest, SimpleFilterManagerTest) {
  util::Region region("test");

  FilterManagerBuilder filter_builder(&region);
  filter_builder.StartNewClause();
  filter_builder.InsertClauseFlavor(TaaT_Lt_500);
  filter_builder.InsertClauseFlavor(Vectorized_Lt_500);

  auto filter = filter_builder.Build(false);
  ASSERT_TRUE(filter != nullptr);

  TableVectorIterator tvi(static_cast<u16>(TableId::Test1));
  for (tvi.Init(); tvi.Advance();) {
    auto *vpi = tvi.vector_projection_iterator();

    // Run the filters
    filter->RunFilters(vpi);

    // Check
    vpi->ForEach([vpi]() {
      auto cola = *vpi->Get<i32, false>(Col::A, nullptr);
      EXPECT_LT(cola, 500);
    });
  }
}

TEST_F(FilterManagerTest, AdaptiveFilterManagerTest) {
  util::Region region("test");

  FilterManagerBuilder filter_builder(&region);
  filter_builder.StartNewClause();
  filter_builder.InsertClauseFlavor(TaaT_Lt_500);
  filter_builder.InsertClauseFlavor(Vectorized_Lt_500);

  auto filter = filter_builder.Build();
  ASSERT_TRUE(filter != nullptr);

  TableVectorIterator tvi(static_cast<u16>(TableId::Test1));
  for (tvi.Init(); tvi.Advance();) {
    auto *vpi = tvi.vector_projection_iterator();

    // Run the filters
    filter->RunFilters(vpi);

    // Check
    vpi->ForEach([vpi]() {
      auto cola = *vpi->Get<i32, false>(Col::A, nullptr);
      EXPECT_LT(cola, 500);
    });
  }
}

}  // namespace tpl::sql::test

#include "sql_test.h"  // NOLINT

#include "catalog/catalog_defs.h"
#include "sql/execution_structures.h"
#include "sql/table_vector_iterator.h"
#include "util/timer.h"

namespace tpl::sql::test {

class TableVectorIteratorTest : public SqlBasedTest {};

TEST_F(TableVectorIteratorTest, EmptyIteratorTest) {
  //
  // Check to see that iteration doesn't begin without an input block
  //
  auto *exec = sql::ExecutionStructures::Instance();
  exec->GetCatalog()->CreateTestTables();
  auto *table = exec->GetCatalog()->LookupTableByName("empty_table");

  TableVectorIterator iter(*table->GetTable(), *table->GetStorageSchema());
  while (iter.Advance()) {
    FAIL() << "Empty table should have no tuples";
  }
}

TEST_F(TableVectorIteratorTest, SimpleIteratorTest) {
  //
  // Simple test to ensure we iterate over the whole table
  //

  auto *exec = sql::ExecutionStructures::Instance();
  exec->GetCatalog()->CreateTestTables();
  auto *table = exec->GetCatalog()->LookupTableByName("test_1");

  TableVectorIterator iter(*table->GetTable(), *table->GetStorageSchema());
  ProjectedColumnsIterator *pci = iter.vector_projection_iterator();

  u32 num_tuples = 0;
  while (iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      num_tuples++;
    }
    pci->Reset();
  }
  EXPECT_EQ(catalog::test_table_size, num_tuples);
}

}  // namespace tpl::sql::test

#include "sql_test.h"  // NOLINT

#include <iostream>
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
  std::cout << "Getting Execution Structures" << std::endl;
  auto *exec = sql::ExecutionStructures::Instance();
  exec->GetCatalog()->CreateTestTables();
  std::cout << "Getting Table" << std::endl;
  auto *table = exec->GetCatalog()->LookupTableByName("empty_table");
  std::cout << "Iterating: " << (table == nullptr) << std::endl;

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
  std::cout << "Read Items = " << num_tuples << std::endl;
  EXPECT_EQ(catalog::test_table_size, num_tuples);
}

}  // namespace tpl::sql::test

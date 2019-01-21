#include "vm/bytecode_handlers.h"

#include "sql/catalog.h"

extern "C" {

void OpSqlTableIteratorInit(tpl::sql::TableIterator *iter, u16 table_id) {
  TPL_ASSERT(iter != nullptr, "Null iterator!");

  auto *table = tpl::sql::Catalog::Instance()->LookupTableById(
      static_cast<tpl::sql::TableId>(table_id));

  // At this point, the table better exist ...
  TPL_ASSERT(table != nullptr, "Table can't be null!");

  new (iter) tpl::sql::TableIterator(*table);
}

void OpSqlTableIteratorClose(tpl::sql::TableIterator *iter) {
  iter->~TableIterator();
}

}  //

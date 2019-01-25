#include "vm/bytecode_handlers.h"

#include "sql/catalog.h"

extern "C" {

void OpTableVectorIteratorInit(tpl::sql::TableVectorIterator *iter,
                               u16 table_id) {
  TPL_ASSERT(iter != nullptr, "Null iterator to initialize");

  auto *table = tpl::sql::Catalog::Instance()->LookupTableById(
      static_cast<tpl::sql::TableId>(table_id));

  // At this point, the table better exist ...
  TPL_ASSERT(table != nullptr, "Table can't be null!");

  new (iter) tpl::sql::TableVectorIterator(*table);
}

void OpTableVectorIteratorClose(tpl::sql::TableVectorIterator *iter) {
  TPL_ASSERT(iter != nullptr, "NULL iterator given to close");
  iter->~TableVectorIterator();
}
void OpVPIFilterEqual(u32 *size, tpl::sql::VectorProjectionIterator *iter,
                      u16 col_id, i64 val) {}

void OpVPIFilterGreaterThan(u32 *size, tpl::sql::VectorProjectionIterator *iter,
                            u16 col_id, i64 val) {}

void OpVPIFilterGreaterThanEqual(u32 *size,
                                 tpl::sql::VectorProjectionIterator *iter,
                                 u16 col_id, i64 val) {}

void OpVPIFilterLessThan(u32 *size, tpl::sql::VectorProjectionIterator *iter,
                         u16 col_id, i64 val) {}

void OpVPIFilterLessThanEqual(u32 *size,
                              tpl::sql::VectorProjectionIterator *iter,
                              u16 col_id, i64 val) {}

void OpVPIFilterNotEqual(u32 *size, tpl::sql::VectorProjectionIterator *iter,
                         u16 col_id, i64 val) {}

}  //

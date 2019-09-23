#include "sql/tablegen/table_generator.h"

#include "csv/csv.hpp"

#include "common/exception.h"
#include "logging/logger.h"
#include "sql/catalog.h"
#include "sql/schema.h"
#include "sql/table.h"
#include "util/bit_util.h"
#include "util/memory.h"
#include "util/timer.h"

namespace tpl::sql::tablegen {

namespace {

std::unique_ptr<Schema> MakeSchema(std::initializer_list<Schema::ColumnInfo> cols) {
  return std::make_unique<Schema>(cols);
}

// Instantiate the table with the given name and schema in the catalog and load its data from a
// file in the provided directory with the same name as the table name.
Table *CreateTable(Catalog *catalog, const std::string &table_name,
                   std::unique_ptr<Schema> schema) {
  const uint16_t table_id = catalog->AllocateTableId();
  catalog->InsertTable(table_name, std::make_unique<Table>(table_id, std::move(schema)));
  LOG_INFO("Instantiated table '{}' in catalog with ID [{}]", table_name, table_id);
  return catalog->LookupTableByName(table_name);
}

// Postgres NULL string
constexpr const char *null_string = "\\N";

void ParseCol(byte *data, uint32_t *null_bitmap, const Schema::ColumnInfo &col, uint32_t row_idx,
              csv::CSVField &field, VarlenHeap *string_heap) {
  if (col.sql_type.nullable()) {
    if (field == null_string) {
      util::BitUtil::Set(null_bitmap, row_idx);
      return;
    }
    util::BitUtil::Unset(null_bitmap, row_idx);
  }

  // Write column data
  byte *insert_offset = data + row_idx * col.GetStorageSize();
  switch (col.sql_type.id()) {
    case SqlTypeId::TinyInt: {
      *reinterpret_cast<int8_t *>(insert_offset) = field.get<int8_t>();
      break;
    }
    case SqlTypeId::SmallInt: {
      *reinterpret_cast<int16_t *>(insert_offset) = field.get<int16_t>();
      break;
    }
    case SqlTypeId::Integer: {
      *reinterpret_cast<int32_t *>(insert_offset) = field.get<int32_t>();
      break;
    }
    case SqlTypeId::BigInt: {
      *reinterpret_cast<int64_t *>(insert_offset) = field.get<int64_t>();
      break;
    }
    case SqlTypeId::Real: {
      *reinterpret_cast<float *>(insert_offset) = field.get<float>();
      break;
    }
    case SqlTypeId::Double: {
      *reinterpret_cast<double *>(insert_offset) = field.get<double>();
      break;
    }
    case SqlTypeId::Date: {
      auto val = field.get<std::string_view>();
      *reinterpret_cast<Date *>(insert_offset) = Date::FromString(val.data(), val.length());
      break;
    }
    case SqlTypeId::Varchar: {
      auto val = field.get<std::string_view>();
      *reinterpret_cast<VarlenEntry *>(insert_offset) =
          string_heap->AddVarlen(val.data(), val.length());
      break;
    }
    default:
      throw NotImplementedException("Parsing column type '{}' not supported",
                                    col.sql_type.GetName());
  }
}

void CreateAndAppendBlockToTable(Table *table,
                                 const std::vector<std::pair<byte *, uint32_t *>> &col_data,
                                 const uint32_t num_vals) {
  TPL_ASSERT(table->schema().num_columns() == col_data.size(), "Missing column data");
  std::vector<ColumnSegment> columns;
  for (uint64_t i = 0; i < col_data.size(); i++) {
    auto col_info = table->schema().GetColumnInfo(i);
    auto [data, null_bitmap] = col_data[i];
    columns.emplace_back(col_info->sql_type, data, null_bitmap, num_vals);
  }
  table->Insert(Table::Block(std::move(columns), num_vals));
}

// If table name is 'test_table', look for a file in data_dir/test_table.tbl
void ImportTable(const std::string &table_name, Table *table, const std::string &data_dir) {
  const uint32_t kBatchSize = 10000;
  uint32_t total_written = 0, num_vals = 0;

  const auto &cols = table->schema().columns();

  VarlenHeap *table_strings = table->mutable_string_heap();
  std::vector<std::pair<byte *, uint32_t *>> col_data;

  util::Timer<std::milli> timer;
  timer.Start();

  const auto data_file = data_dir + "/" + table_name + ".tbl";
  auto reader = csv::CSVReader(data_file, csv::CSVFormat().delimiter({'|', '\n'}));
  for (csv::CSVRow &row : reader) {
    if (num_vals == 0) {
      for (const auto &col : table->schema().columns()) {
        byte *data = static_cast<byte *>(
            util::MallocAligned(col.GetStorageSize() * kBatchSize, CACHELINE_SIZE));
        uint32_t *nulls = nullptr;
        if (col.sql_type.nullable()) {
          nulls = static_cast<uint32_t *>(util::MallocAligned(
              util::BitUtil::Num32BitWordsFor(kBatchSize) * sizeof(uint32_t), CACHELINE_SIZE));
        }
        col_data.emplace_back(data, nulls);
      }
    }

    // Write table data
    for (uint32_t col_idx = 0; col_idx < table->schema().num_columns(); col_idx++) {
      auto field = row[col_idx];
      ParseCol(col_data[col_idx].first, col_data[col_idx].second, cols[col_idx], num_vals, field,
               table_strings);
    }

    num_vals++;

    // If we've reached batch size, construct block and append to table
    if (num_vals == kBatchSize) {
      CreateAndAppendBlockToTable(table, col_data, num_vals);
      col_data.clear();
      total_written += num_vals;
      num_vals = 0;
    }
  }

  // Last block
  if (num_vals > 0) {
    CreateAndAppendBlockToTable(table, col_data, num_vals);
    total_written += num_vals;
  }

  timer.Stop();

  auto rps = total_written / timer.elapsed() * 1000.0;
  LOG_INFO("Loaded '{}' with {} rows ({:.2f} rows/sec)", table_name, total_written, rps);
}

}  // namespace

void TableGenerator::GenerateTPCHTables(Catalog *catalog, const std::string &data_dir) {
  LOG_INFO("Loading TPC-H tables ...");

  // -------------------------------------------------------
  //
  // Customer
  //
  // -------------------------------------------------------

  {
    auto table_name = std::string("customer");
    auto schema = MakeSchema({
        {"c_custkey", IntegerType::InstanceNonNullable()},
        {"c_name", VarcharType::InstanceNonNullable(25)},
        {"c_address", VarcharType::InstanceNonNullable(40)},
        {"c_nationkey", IntegerType::InstanceNonNullable()},
        {"c_phone", VarcharType::InstanceNonNullable(15)},
        {"c_acctbal", RealType::InstanceNonNullable()},
        {"c_mktsegment", VarcharType::InstanceNonNullable(10)},
        {"c_comment", VarcharType::InstanceNonNullable(117)},
    });
    auto table = CreateTable(catalog, table_name, std::move(schema));
    ImportTable(table_name, table, data_dir);
  }

  // -------------------------------------------------------
  //
  // Part
  //
  // -------------------------------------------------------

  {
    auto table_name = "part";
    auto schema = MakeSchema({{"p_partkey", IntegerType::InstanceNonNullable()},
                              {"p_name", VarcharType::InstanceNonNullable(55)},
                              {"p_mfgr", VarcharType::InstanceNonNullable(25)},
                              {"p_brand", VarcharType::InstanceNonNullable(10)},
                              {"p_type", VarcharType::InstanceNonNullable(25)},
                              {"p_size", IntegerType::InstanceNonNullable()},
                              {"p_container", VarcharType::InstanceNonNullable(10)},
                              {"p_retailprice", RealType::InstanceNonNullable()},
                              {"p_comment", VarcharType::InstanceNonNullable(23)}});
    auto table = CreateTable(catalog, table_name, std::move(schema));
    ImportTable(table_name, table, data_dir);
  }

  // -------------------------------------------------------
  //
  // Supplier
  //
  // -------------------------------------------------------

  {
    auto table_name = "supplier";
    auto schema = MakeSchema({{"s_suppkey", IntegerType::InstanceNonNullable()},
                              {"s_name", VarcharType::InstanceNonNullable(25)},
                              {"s_address", VarcharType::InstanceNonNullable(40)},
                              {"s_nationkey", IntegerType::InstanceNonNullable()},
                              {"s_phone", VarcharType::InstanceNonNullable(15)},
                              {"s_acctbal", RealType::InstanceNonNullable()},
                              {"s_comment", VarcharType::InstanceNonNullable(101)}});
    auto table = CreateTable(catalog, table_name, std::move(schema));
    ImportTable(table_name, table, data_dir);
  }

  // -------------------------------------------------------
  //
  // Partsupp
  //
  // -------------------------------------------------------

  {
    auto table_name = "partsupp";
    auto schema = MakeSchema({{"ps_partkey", IntegerType::InstanceNonNullable()},
                              {"ps_suppkey", IntegerType::InstanceNonNullable()},
                              {"ps_availqty", IntegerType::InstanceNonNullable()},
                              {"ps_supplycost", RealType::InstanceNonNullable()},
                              {"ps_comment", VarcharType::InstanceNonNullable(199)}});
    auto table = CreateTable(catalog, table_name, std::move(schema));
    ImportTable(table_name, table, data_dir);
  }

  // -------------------------------------------------------
  //
  // Orders
  //
  // -------------------------------------------------------

  {
    auto table_name = "orders";
    auto schema = MakeSchema({{"o_orderkey", IntegerType::InstanceNonNullable()},
                              {"o_custkey", IntegerType::InstanceNonNullable()},
                              {"o_orderstatus", VarcharType::InstanceNonNullable(1)},
                              {"o_totalprice", RealType::InstanceNonNullable()},
                              {"o_orderdate", DateType::InstanceNonNullable()},
                              {"o_orderpriority", VarcharType::InstanceNonNullable(15)},
                              {"o_clerk", VarcharType::InstanceNonNullable(15)},
                              {"o_shippriority", IntegerType::InstanceNonNullable()},
                              {"o_comment", VarcharType::InstanceNonNullable(79)}});
    auto table = CreateTable(catalog, table_name, std::move(schema));
    ImportTable(table_name, table, data_dir);
  }

  // -------------------------------------------------------
  //
  // Lineitem
  //
  // -------------------------------------------------------

  {
    auto table_name = "lineitem";
    auto schema = MakeSchema({{"l_orderkey", IntegerType::InstanceNonNullable()},
                              {"l_partkey", IntegerType::InstanceNonNullable()},
                              {"l_suppkey", IntegerType::InstanceNonNullable()},
                              {"l_linenumber", IntegerType::InstanceNonNullable()},
                              {"l_quantity", RealType::InstanceNonNullable()},
                              {"l_extendedprice", RealType::InstanceNonNullable()},
                              {"l_discount", RealType::InstanceNonNullable()},
                              {"l_tax", RealType::InstanceNonNullable()},
                              {"l_returnflag", VarcharType::InstanceNonNullable(1)},
                              {"l_linestatus", VarcharType::InstanceNonNullable(1)},
                              {"l_shipdate", DateType::InstanceNonNullable()},
                              {"l_commitdate", DateType::InstanceNonNullable()},
                              {"l_receiptdate", DateType::InstanceNonNullable()},
                              {"l_shipinstruct", VarcharType::InstanceNonNullable(25)},
                              {"l_shipmode", VarcharType::InstanceNonNullable(10)},
                              {"l_comment", VarcharType::InstanceNonNullable(44)}});

    auto table = CreateTable(catalog, table_name, std::move(schema));
    ImportTable(table_name, table, data_dir);
  }

  // -------------------------------------------------------
  //
  // Nation
  //
  // -------------------------------------------------------

  {
    auto table_name = "nation";
    auto schema = MakeSchema({{"n_nationkey", IntegerType::InstanceNonNullable()},
                              {"n_name", VarcharType::InstanceNonNullable(25)},
                              {"n_regionkey", IntegerType::InstanceNonNullable()},
                              {"n_comment", VarcharType::InstanceNonNullable(152)}});
    auto table = CreateTable(catalog, table_name, std::move(schema));
    ImportTable(table_name, table, data_dir);
  }

  // -------------------------------------------------------
  //
  // Region
  //
  // -------------------------------------------------------

  {
    auto table_name = "region";
    auto schema = MakeSchema({{"r_regionkey", IntegerType::InstanceNonNullable()},
                              {"r_name", VarcharType::InstanceNonNullable(25)},
                              {"r_comment", VarcharType::InstanceNonNullable(152)}});
    auto table = CreateTable(catalog, table_name, std::move(schema));
    ImportTable(table_name, table, data_dir);
  }

  LOG_INFO("Completed loading TPC-H tables ...");
}

}  // namespace tpl::sql::tablegen

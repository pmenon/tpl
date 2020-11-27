#include "sql/tablegen/table_generator.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "csv/csv.hpp"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "common/memory.h"
#include "logging/logger.h"
#include "sql/catalog.h"
#include "sql/schema.h"
#include "sql/table.h"
#include "util/bit_util.h"
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
  catalog->InsertTable(table_name,
                       std::make_unique<Table>(table_id, table_name, std::move(schema)));
  LOG_INFO("Instantiated table '{}' in catalog with ID [{}]", table_name, table_id);
  return catalog->LookupTableByName(table_name);
}

// Postgres NULL string
constexpr const char *kNullString = "\\N";

void ParseCol(byte *data, uint32_t *null_bitmap, const Schema::ColumnInfo &col, uint32_t row_idx,
              csv::CSVField &field, VarlenHeap *string_heap) {
  if (col.type.IsNullable()) {
    if (field == kNullString) {
      util::BitUtil::Set(null_bitmap, row_idx);
      return;
    }
    util::BitUtil::Unset(null_bitmap, row_idx);
  }

  // Write column data
  byte *insert_offset = data + row_idx * col.GetStorageSize();
  switch (col.type.GetTypeId()) {
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
      throw NotImplementedException(fmt::format("Parsing column type '{}' not supported",
                                                col.type.ToStringWithoutNullability()));
  }
}

void CreateAndAppendBlockToTable(Table *table,
                                 const std::vector<std::pair<byte *, uint32_t *>> &col_data,
                                 const uint32_t num_vals) {
  TPL_ASSERT(table->GetSchema().GetColumnCount() == col_data.size(), "Missing column data");
  std::vector<ColumnSegment> columns;
  for (uint64_t i = 0; i < col_data.size(); i++) {
    auto col_info = table->GetSchema().GetColumnInfo(i);
    auto [data, null_bitmap] = col_data[i];
    columns.emplace_back(col_info->type, data, null_bitmap, num_vals);
  }
  table->Insert(Table::Block(std::move(columns), num_vals));
}

// If table name is 'test_table', look for a file in data_dir/test_table.tbl
void ImportTable(const std::string &table_name, Table *table, const std::string &data_dir) {
  const uint32_t kBatchSize = 10000;
  uint32_t total_written = 0, num_vals = 0;

  const auto &cols = table->GetSchema().GetColumns();

  VarlenHeap *table_strings = table->GetMutableStringHeap();
  std::vector<std::pair<byte *, uint32_t *>> col_data;

  util::Timer<std::milli> timer;
  timer.Start();

  std::vector<std::string> col_names;
  for (const auto &col : cols) col_names.push_back(col.name);

  const auto data_file = data_dir + "/" + table_name + ".csv";
  auto reader = csv::CSVReader(data_file, csv::CSVFormat{}.delimiter('|').column_names(col_names));
  for (csv::CSVRow &row : reader) {
    if (num_vals == 0) {
      for (const auto &col : table->GetSchema().GetColumns()) {
        byte *data = static_cast<byte *>(
            Memory::MallocAligned(col.GetStorageSize() * kBatchSize, CACHELINE_SIZE));
        uint32_t *nulls = nullptr;
        if (col.type.IsNullable()) {
          nulls = static_cast<uint32_t *>(Memory::MallocAligned(
              util::BitUtil::Num32BitWordsFor(kBatchSize) * sizeof(uint32_t), CACHELINE_SIZE));
        }
        col_data.emplace_back(data, nulls);
      }
    }

    // Write table data
    for (uint32_t col_idx = 0; col_idx < table->GetSchema().GetColumnCount(); col_idx++) {
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

  auto rps = total_written / timer.GetElapsed() * 1000.0;
  LOG_INFO("Loaded '{}' with {} rows ({:.2f} rows/sec)", table_name, total_written, rps);
}

}  // namespace

void TableGenerator::GenerateTPCHTables(Catalog *catalog, const std::string &data_dir,
                                        bool compress) {
  LOG_INFO("Loading TPC-H {} tables ...", compress ? "compressed" : "");

  // -------------------------------------------------------
  //
  // Customer
  //
  // -------------------------------------------------------

  {
    auto schema = MakeSchema({
        {"c_custkey", Type::IntegerType(false)},
        {"c_name", Type::VarcharType(false, 25)},
        {"c_address", Type::VarcharType(false, 40)},
        {"c_nationkey", Type::IntegerType(false)},
        {"c_phone", Type::VarcharType(false, 15)},
        {"c_acctbal", Type::RealType(false)},
        {"c_mktsegment", Type::VarcharType(false, 10)},
        {"c_comment", Type::VarcharType(false, 117)},
    });
    auto table = CreateTable(catalog, "tpch.customer", std::move(schema));
    ImportTable("customer", table, data_dir);
  }

  // -------------------------------------------------------
  //
  // Part
  //
  // -------------------------------------------------------

  {
    auto schema = MakeSchema({{"p_partkey", Type::IntegerType(false)},
                              {"p_name", Type::VarcharType(false, 55)},
                              {"p_mfgr", Type::VarcharType(false, 25)},
                              {"p_brand", Type::VarcharType(false, 10)},
                              {"p_type", Type::VarcharType(false, 25)},
                              {"p_size", Type::IntegerType(false)},
                              {"p_container", Type::VarcharType(false, 10)},
                              {"p_retailprice", Type::RealType(false)},
                              {"p_comment", Type::VarcharType(false, 23)}});
    auto table = CreateTable(catalog, "tpch.part", std::move(schema));
    ImportTable("part", table, data_dir);
  }

  // -------------------------------------------------------
  //
  // Supplier
  //
  // -------------------------------------------------------

  {
    auto schema = MakeSchema({{"s_suppkey", Type::IntegerType(false)},
                              {"s_name", Type::VarcharType(false, 25)},
                              {"s_address", Type::VarcharType(false, 40)},
                              {"s_nationkey", Type::IntegerType(false)},
                              {"s_phone", Type::VarcharType(false, 15)},
                              {"s_acctbal", Type::RealType(false)},
                              {"s_comment", Type::VarcharType(false, 101)}});
    auto table = CreateTable(catalog, "tpch.supplier", std::move(schema));
    ImportTable("supplier", table, data_dir);
  }

  // -------------------------------------------------------
  //
  // Partsupp
  //
  // -------------------------------------------------------

  {
    auto schema = MakeSchema({{"ps_partkey", Type::IntegerType(false)},
                              {"ps_suppkey", Type::IntegerType(false)},
                              {"ps_availqty", Type::IntegerType(false)},
                              {"ps_supplycost", Type::RealType(false)},
                              {"ps_comment", Type::VarcharType(false, 199)}});
    auto table = CreateTable(catalog, "tpch.partsupp", std::move(schema));
    ImportTable("partsupp", table, data_dir);
  }

  // -------------------------------------------------------
  //
  // Orders
  //
  // -------------------------------------------------------

  {
    auto schema = MakeSchema({{"o_orderkey", Type::IntegerType(false)},
                              {"o_custkey", Type::IntegerType(false)},
                              {"o_orderstatus", Type::VarcharType(false, 1)},
                              {"o_totalprice", Type::RealType(false)},
                              {"o_orderdate", Type::Type::DateType(false)},
                              {"o_orderpriority", Type::VarcharType(false, 15)},
                              {"o_clerk", Type::VarcharType(false, 15)},
                              {"o_shippriority", Type::IntegerType(false)},
                              {"o_comment", Type::VarcharType(false, 79)}});
    auto table = CreateTable(catalog, "tpch.orders", std::move(schema));
    ImportTable("orders", table, data_dir);
  }

  // -------------------------------------------------------
  //
  // Lineitem
  //
  // -------------------------------------------------------

  {
    auto schema = MakeSchema({{"l_orderkey", Type::IntegerType(false)},
                              {"l_partkey", Type::IntegerType(false)},
                              {"l_suppkey", Type::IntegerType(false)},
                              {"l_linenumber", Type::IntegerType(false)},
                              {"l_quantity", Type::RealType(false)},
                              {"l_extendedprice", Type::RealType(false)},
                              {"l_discount", Type::RealType(false)},
                              {"l_tax", Type::RealType(false)},
                              {"l_returnflag", Type::VarcharType(false, 1)},
                              {"l_linestatus", Type::VarcharType(false, 1)},
                              {"l_shipdate", Type::DateType(false)},
                              {"l_commitdate", Type::DateType(false)},
                              {"l_receiptdate", Type::DateType(false)},
                              {"l_shipinstruct", Type::VarcharType(false, 25)},
                              {"l_shipmode", Type::VarcharType(false, 10)},
                              {"l_comment", Type::VarcharType(false, 44)}});

    auto table = CreateTable(catalog, "tpch.lineitem", std::move(schema));
    ImportTable("lineitem", table, data_dir);
  }

  // -------------------------------------------------------
  //
  // Nation
  //
  // -------------------------------------------------------

  {
    auto schema = MakeSchema({{"n_nationkey", Type::IntegerType(false)},
                              {"n_name", Type::VarcharType(false, 25)},
                              {"n_regionkey", Type::IntegerType(false)},
                              {"n_comment", Type::VarcharType(false, 152)}});
    auto table = CreateTable(catalog, "tpch.nation", std::move(schema));
    ImportTable("nation", table, data_dir);
  }

  // -------------------------------------------------------
  //
  // Region
  //
  // -------------------------------------------------------

  {
    auto schema = MakeSchema({{"r_regionkey", Type::IntegerType(false)},
                              {"r_name", Type::VarcharType(false, 25)},
                              {"r_comment", Type::VarcharType(false, 152)}});
    auto table = CreateTable(catalog, "tpch.region", std::move(schema));
    ImportTable("region", table, data_dir);
  }

  LOG_INFO("Completed loading TPC-H tables ...");
}

void TableGenerator::GenerateSSBMTables(sql::Catalog *catalog, const std::string &data_dir) {
  LOG_INFO("Loading SSBM tables ...");

  // -------------------------------------------------------
  // Part
  // -------------------------------------------------------

  {
    auto schema = MakeSchema({
        {"p_partkey", Type::IntegerType(false)},
        {"p_name", Type::VarcharType(false, 22)},
        {"p_mfgr", Type::VarcharType(false, 6)},
        {"p_category", Type::VarcharType(false, 7)},
        {"p_brand1", Type::VarcharType(false, 9)},
        {"p_color", Type::VarcharType(false, 11)},
        {"p_type", Type::VarcharType(false, 25)},
        {"p_size", Type::IntegerType(false)},
        {"p_container", Type::VarcharType(false, 10)},
    });
    auto table = CreateTable(catalog, "ssbm.part", std::move(schema));
    ImportTable("part", table, data_dir);
  }

  // -------------------------------------------------------
  // Supplier
  // -------------------------------------------------------

  {
    auto schema = MakeSchema({
        {"s_suppkey", Type::IntegerType(false)},
        {"s_name", Type::VarcharType(false, 25)},
        {"s_address", Type::VarcharType(false, 25)},
        {"s_city", Type::VarcharType(false, 10)},
        {"s_nation", Type::VarcharType(false, 15)},
        {"s_region", Type::VarcharType(false, 12)},
        {"s_phone", Type::VarcharType(false, 15)},
    });
    auto table = CreateTable(catalog, "ssbm.supplier", std::move(schema));
    ImportTable("supplier", table, data_dir);
  }

  // -------------------------------------------------------
  // Customer
  // -------------------------------------------------------

  {
    auto schema = MakeSchema({
        {"c_custkey", Type::IntegerType(false)},
        {"c_name", Type::VarcharType(false, 25)},
        {"c_address", Type::VarcharType(false, 25)},
        {"c_city", Type::VarcharType(false, 10)},
        {"c_nation", Type::VarcharType(false, 15)},
        {"c_region", Type::VarcharType(false, 12)},
        {"c_phone", Type::VarcharType(false, 15)},
        {"c_mktsegment", Type::VarcharType(false, 10)},
    });
    auto table = CreateTable(catalog, "ssbm.customer", std::move(schema));
    ImportTable("customer", table, data_dir);
  }

  // -------------------------------------------------------
  // Date
  // -------------------------------------------------------

  {
    auto schema = MakeSchema({
        {"d_datekey", Type::IntegerType(false)},
        {"d_date", Type::VarcharType(false, 19)},
        {"d_dayofweek", Type::VarcharType(false, 10)},
        {"d_month", Type::VarcharType(false, 10)},
        {"d_year", Type::IntegerType(false)},
        {"d_yearmonthnum", Type::IntegerType(false)},
        {"d_yearmonth", Type::VarcharType(false, 8)},
        {"d_daynuminweek", Type::IntegerType(false)},
        {"d_daynuminmonth", Type::IntegerType(false)},
        {"d_daynuminyear", Type::IntegerType(false)},
        {"d_monthnuminyear", Type::IntegerType(false)},
        {"d_weeknuminyear", Type::IntegerType(false)},
        {"d_sellingseason", Type::VarcharType(false, 13)},
        {"d_lasdayinweekfl", Type::VarcharType(false, 1)},
        {"d_lastdayinmonthfl", Type::VarcharType(false, 1)},
        {"d_holidyfl", Type::VarcharType(false, 1)},
        {"d_weekdayfl", Type::VarcharType(false, 1)},
    });
    auto table = CreateTable(catalog, "ssbm.date", std::move(schema));
    ImportTable("date", table, data_dir);
  }

  // -------------------------------------------------------
  // Line-Order
  // -------------------------------------------------------

  {
    auto schema = MakeSchema({
        {"lo_orderkey", Type::IntegerType(false)},
        {"lo_linenumber", Type::IntegerType(false)},
        {"lo_custkey", Type::IntegerType(false)},
        {"lo_partkey", Type::IntegerType(false)},
        {"lo_suppkey", Type::IntegerType(false)},
        {"lo_orderdate", Type::IntegerType(false)},
        {"lo_orderpriority", Type::VarcharType(false, 15)},
        {"lo_shippriority", Type::VarcharType(false, 1)},
        {"lo_quantity", Type::IntegerType(false)},
        {"lo_extendedprice", Type::IntegerType(false)},
        {"lo_ordertotalprice", Type::IntegerType(false)},
        {"lo_discount", Type::IntegerType(false)},
        {"lo_revenue", Type::IntegerType(false)},
        {"lo_supplycost", Type::IntegerType(false)},
        {"lo_tax", Type::IntegerType(false)},
        {"lo_commitdate", Type::IntegerType(false)},
        {"lo_shipmode", Type::VarcharType(false, 10)},
    });
    auto table = CreateTable(catalog, "ssbm.lineorder", std::move(schema));
    ImportTable("lineorder", table, data_dir);
  }

  LOG_INFO("Completed loading SSBM tables ...");
}

}  // namespace tpl::sql::tablegen

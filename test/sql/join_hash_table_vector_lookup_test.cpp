#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include "tpl_test.h"  // NOLINT

#include "catalog/catalog.h"
#include "sql/data_types.h"
#include "sql/execution_structures.h"
#include "sql/join_hash_table.h"
#include "sql/join_hash_table_vector_lookup.h"
#include "sql/projected_columns_iterator.h"
#include "storage/projected_columns.h"
#include "util/hash.h"

namespace tpl::sql::test {
using namespace terrier;
/// This is the tuple we insert into the hash table
template <u8 N>
struct Tuple {
  u32 build_key;
  u32 aux[N];
};

template <u8 N>
static inline hash_t HashTupleInPCI(ProjectedColumnsIterator *pci) noexcept {
  const auto *key_ptr = pci->Get<u32, false>(0, nullptr);
  return util::Hasher::Hash(reinterpret_cast<const u8 *>(key_ptr),
                            sizeof(Tuple<N>::build_key));
}

/// The function to determine whether two tuples have equivalent keys
template <u8 N>
static inline bool CmpTupleInPCI(const byte *table_tuple,
                                 ProjectedColumnsIterator *pci) noexcept {
  auto lhs_key = reinterpret_cast<const Tuple<N> *>(table_tuple)->build_key;
  auto rhs_key = *pci->Get<u32, false>(0, nullptr);
  return lhs_key == rhs_key;
}

class JoinHashTableVectorLookupTest : public TplTest {
 public:
  JoinHashTableVectorLookupTest() : region_(GetTestName()) {
    InitializeColumns();
  }

  void InitializeColumns() {
    // TODO(Amadou): Find easier way to make a ProjectedColumns.
    // This should be done after the catalog PR is in.

    // Create two columns.
    auto *exec = sql::ExecutionStructures::Instance();
    auto *ctl = exec->GetCatalog();
    catalog::Schema::Column storage_col1 =
        ctl->MakeStorageColumn("col1", sql::IntegerType::Instance(false));
    catalog::Schema::Column storage_col2 =
        ctl->MakeStorageColumn("col2", sql::IntegerType::Instance(false));
    sql::Schema::ColumnInfo sql_col1("col1", sql::IntegerType::Instance(false));
    sql::Schema::ColumnInfo sql_col2("col2", sql::IntegerType::Instance(false));

    // Create the table
    catalog::Schema storage_schema(
        std::vector<catalog::Schema::Column>{storage_col1, storage_col2});
    sql::Schema sql_schema(
        std::vector<sql::Schema::ColumnInfo>{sql_col1, sql_col2});
    ctl->CreateTable("hash_test_table", std::move(storage_schema),
                     std::move(sql_schema));

    // Get the table's info
    info_ = ctl->LookupTableByName("hash_test_table");
    std::vector<catalog::col_oid_t> col_oids;
    for (const auto &col : info_->GetStorageSchema()->GetColumns())
      col_oids.emplace_back(col.GetOid());
    auto initializer_map = info_->GetTable()->InitializerForProjectedColumns(
        col_oids, kDefaultVectorSize);

    // Create the ProjectedColumns
    buffer_ = common::AllocationUtil::AllocateAligned(
        initializer_map.first.ProjectedColumnsSize());
    projected_columns = initializer_map.first.Initialize(buffer_);
    projected_columns->SetNumTuples(kDefaultVectorSize);
  }

  util::Region *region() { return &region_; }

  storage::ProjectedColumns *projected_columns = nullptr;

 private:
  util::Region region_;
  catalog::Catalog::TableInfo *info_ = nullptr;
  byte *buffer_ = nullptr;
};

template <u8 N, typename F>
std::unique_ptr<const JoinHashTable> InsertAndBuild(util::Region *region,
                                                    bool concise,
                                                    u32 num_tuples,
                                                    F &&key_gen) {
  auto jht = std::make_unique<JoinHashTable>(region, sizeof(Tuple<N>), concise);

  // Insert
  for (u32 i = 0; i < num_tuples; i++) {
    auto key = key_gen();
    auto hash =
        util::Hasher::Hash(reinterpret_cast<const u8 *>(&key), sizeof(key));
    auto *tuple = reinterpret_cast<Tuple<N> *>(jht->AllocInputTuple(hash));
    tuple->build_key = key;
  }

  // Build
  jht->Build();

  // Finish
  return jht;
}

// Sequential number functor
struct Seq {
  u32 c;
  explicit Seq(u32 cc) : c(cc) {}
  u32 operator()() noexcept { return c++; }
};

struct Range {
  std::random_device random;
  std::uniform_int_distribution<u32> dist;
  Range(u32 min, u32 max) : dist(min, max) {}
  u32 operator()() noexcept { return dist(random); }
};

// Random number functor
struct Rand {
  std::random_device random;
  Rand() = default;
  u32 operator()() noexcept { return random(); }
};

TEST_F(JoinHashTableVectorLookupTest, SimpleGenericLookupTest) {
  constexpr const u8 N = 1;
  constexpr const u32 num_build = 1000;
  constexpr const u32 num_probe = num_build * 10;

  // Create test JHT
  auto jht = InsertAndBuild<N>(region(), /*concise*/ false, num_build, Seq(0));

  // Create test probe input
  auto probe_keys = std::vector<u32>(num_probe);
  std::generate(probe_keys.begin(), probe_keys.end(), Range(0, num_build - 1));

  ProjectedColumnsIterator pci(projected_columns);

  // Lookup
  JoinHashTableVectorLookup lookup(*jht);

  // Loop over all matches
  u32 count = 0;
  for (u32 i = 0; i < num_probe; i += projected_columns->MaxTuples()) {
    u32 size = std::min(projected_columns->MaxTuples(), num_probe - i);

    // Setup Projected Column
    projected_columns->SetNumTuples(size);
    std::memcpy(projected_columns->ColumnStart(0), &probe_keys[i], size);
    pci.SetProjectedColumn(projected_columns);

    // Lookup
    lookup.Prepare(&pci, HashTupleInPCI<N>);

    // Iterate all
    while (const auto *entry = lookup.GetNextOutput(&pci, CmpTupleInPCI<N>)) {
      count++;
      auto ht_key = entry->PayloadAs<Tuple<N>>()->build_key;
      auto probe_key = *pci.Get<u32, false>(0, nullptr);
      EXPECT_EQ(ht_key, probe_key);
    }
  }

  EXPECT_EQ(num_probe, count);
}

TEST_F(JoinHashTableVectorLookupTest, DISABLED_PerfLookupTest) {
  auto bench = [this](bool concise) {
    constexpr const u8 N = 1;
    constexpr const u32 num_build = 5000000;
    constexpr const u32 num_probe = num_build * 10;

    // Create test JHT
    auto jht = InsertAndBuild<N>(region(), concise, num_build, Seq(0));

    // Create test probe input
    auto probe_keys = std::vector<u32>(num_probe);
    std::generate(probe_keys.begin(), probe_keys.end(),
                  Range(0, num_build - 1));

    ProjectedColumnsIterator pci(projected_columns);

    // Lookup
    JoinHashTableVectorLookup lookup(*jht);

    util::Timer<std::milli> timer;
    timer.Start();

    // Loop over all matches
    u32 count = 0;
    for (u32 i = 0; i < num_probe; i += kDefaultVectorSize) {
      u32 size = std::min(kDefaultVectorSize, num_probe - i);

      // Setup Projected Column
      projected_columns->SetNumTuples(size);
      std::memcpy(projected_columns->ColumnStart(0), &probe_keys[i], size);
      pci.SetProjectedColumn(projected_columns);

      // Lookup
      lookup.Prepare(&pci, HashTupleInPCI<N>);

      // Iterate all
      while (const auto *entry = lookup.GetNextOutput(&pci, CmpTupleInPCI<N>)) {
        (void)entry;
        count++;
      }
    }

    timer.Stop();
    auto mtps = (num_probe / timer.elapsed()) / 1000.0;
    LOG_INFO("========== {} ==========", concise ? "Concise" : "Generic");
    LOG_INFO("# Probes    : {}", num_probe)
    LOG_INFO("Probe Time  : {} ms ({:.2f} Mtps)", timer.elapsed(), mtps);
  };

  bench(false);
  bench(true);
}

}  // namespace tpl::sql::test

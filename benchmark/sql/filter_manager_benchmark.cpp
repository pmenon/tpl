#include <fstream>
#include <iomanip>
#include <mutex>

#include "benchmark/benchmark.h"
#include "common/exception.h"
#include "common/memory.h"
#include "common/settings.h"
#include "logging/logger.h"
#include "sql/catalog.h"
#include "sql/constant_vector.h"
#include "sql/filter_manager.h"
#include "sql/schema.h"
#include "sql/table.h"
#include "sql/table_vector_iterator.h"
#include "sql/vector_operations/vector_operators.h"
#include "util/timer.h"

////////////////////////////////////////////////////////////////////////////////
///
/// All benchmarks use the same predicate:
/// a < 10 AND b < 20 AND c < 30
///
/// We generate different tables that produce different overall selectivities
/// given the predicate above.
///
////////////////////////////////////////////////////////////////////////////////

namespace tpl {

namespace {

// The number of runs to perform and average over.
constexpr const uint32_t kNumRuns = 10;

// The constants used in the predicate.
constexpr const int64_t kPredA = 10;
constexpr const int64_t kPredB = 20;
constexpr const int64_t kPredC = 30;

// The filtering terms: A < 10 AND B < 20 AND C < 30
constexpr const sql::FilterManager::MatchFn kAllTerms[] = {
    [](auto input, auto tids, auto ctx) {
      auto c_vec = input->GetColumn(2);
      auto c_val = sql::GenericValue::CreateInteger(kPredC);
      sql::VectorOps::SelectLessThan(*c_vec, sql::ConstantVector(c_val), tids);
    },
    [](auto input, auto tids, auto ctx) {
      auto b_vec = input->GetColumn(1);
      auto b_val = sql::GenericValue::CreateInteger(kPredB);
      sql::VectorOps::SelectLessThan(*b_vec, sql::ConstantVector(b_val), tids);
    },
    [](auto input, auto tids, auto ctx) {
      auto a_vec = input->GetColumn(0);
      auto a_val = sql::GenericValue::CreateInteger(kPredA);
      sql::VectorOps::SelectLessThan(*a_vec, sql::ConstantVector(a_val), tids);
    },
};

// Get a specific ordering of filtering terms.
std::vector<sql::FilterManager::MatchFn> GetTermsByOrder(const std::vector<uint32_t> &term_order) {
  std::vector<sql::FilterManager::MatchFn> result;
  for (auto i : term_order) result.push_back(kAllTerms[i]);
  return result;
}

// Metadata about a table.
struct TableMeta {
  const char *name;
  double selectivity;
  double col_selectivities[3];
};

constexpr const uint32_t kNumTableRows = 3000000;

// This array lists component selectivities for each column A, B, C required to
// produced a desired overall selectivity.
constexpr const TableMeta kTables[] = {
    {"FM_00", 0.00, {0.0, 0.98, 0.98}},       // 0%
    {"FM_02", 0.02, {0.020824, 0.98, 0.98}},  // 2%
    {"FM_05", 0.05, {0.052061, 0.98, 0.98}},  // 5%
    {"FM_10", 0.10, {0.104123, 0.98, 0.98}},  // 10%
    {"FM_15", 0.15, {0.156184, 0.98, 0.98}},  // 15%
    {"FM_20", 0.20, {0.208246, 0.98, 0.98}},  // 20%
    {"FM_25", 0.25, {0.260308, 0.98, 0.98}},  // 25%
    {"FM_30", 0.30, {0.312369, 0.98, 0.98}},  // 30%
    {"FM_35", 0.35, {0.364431, 0.98, 0.98}},  // 35%
    {"FM_40", 0.40, {0.416493, 0.98, 0.98}},  // 40%
    {"FM_45", 0.45, {0.468554, 0.98, 0.98}},  // 45%
    {"FM_50", 0.50, {0.520616, 0.98, 0.98}},  // 50%
    {"FM_55", 0.55, {0.572678, 0.98, 0.98}},  // 55%
    {"FM_60", 0.60, {0.624739, 0.98, 0.98}},  // 60%
    {"FM_65", 0.65, {0.676801, 0.98, 0.98}},  // 65%
    {"FM_70", 0.70, {0.728862, 0.98, 0.98}},  // 70%
    {"FM_75", 0.75, {0.780924, 0.98, 0.98}},  // 75%
    {"FM_80", 0.80, {0.832986, 0.98, 0.98}},  // 80%
    {"FM_85", 0.85, {0.885047, 0.98, 0.98}},  // 85%
    {"FM_90", 0.90, {0.937109, 0.98, 0.98}},  // 90%
    {"FM_95", 0.95, {0.989171, 0.98, 0.98}},  // 95%
    {"FM_100", 1.00, {1.00, 1.00, 1.00}},     // 100%
};

constexpr const std::size_t kNumTables = sizeof(kTables) / sizeof(kTables[0]);

// Generate a column's data using a uniform random distribution between min/max.
int32_t *GenColumnData(uint32_t num_vals, int32_t min, int32_t max) {
  auto *data =
      static_cast<int32_t *>(Memory::MallocAligned(sizeof(int32_t) * num_vals, CACHELINE_SIZE));
  std::mt19937 generator(std::random_device{}());
  std::uniform_int_distribution<int32_t> distribution(min, max);
  if (min == max) {
    for (uint32_t i = 0; i < num_vals; i++) {
      data[i] = max;
    }
  } else {
    for (uint32_t i = 0; i < num_vals; i++) {
      data[i] = distribution(generator);
    }
  }

  return data;
}

// Load the given test table using the provided selectivities for A, B, and C.
// The selectivities are rotated across the columns in three phases.
void LoadTestTable(sql::Table *table, uint32_t num_rows, double s1, double s2, double s3) {
  const uint32_t num_phases = 3;
  const uint32_t batch_size = 10000;
  const uint32_t num_batches = num_rows / batch_size;
  const uint32_t num_batches_per_phase = num_batches / num_phases;
  if (num_rows % batch_size != 0 || num_rows % num_phases != 0) {
    throw NotImplementedException("Number of rows ({}) must be a multiple of batch size ({})",
                                  num_rows, batch_size);
  }

  int64_t min_a, max_a;
  int64_t min_b, max_b;
  int64_t min_c, max_c;
  for (uint32_t i = 0; i < num_phases; i++) {
    if (i == 0) {
      // A
      min_a = s1 == 0.0 ? kPredA + 1 : 0;
      max_a = s1 == 0.0 ? min_a + 10 : static_cast<int64_t>(kPredA / s1);
      // B
      min_b = s2 == 0.0 ? kPredB + 1 : 0;
      max_b = s2 == 0.0 ? min_b + 10 : static_cast<int64_t>(kPredB / s2);
      // C
      min_c = s3 == 0.0 ? kPredC + 1 : 0;
      max_c = s3 == 0.0 ? min_c + 10 : static_cast<int64_t>(kPredC / s3);
    } else if (i == 1) {
      // A
      min_a = s2 == 0.0 ? kPredA + 1 : 0;
      max_a = s2 == 0.0 ? min_a + 10 : static_cast<int64_t>(kPredA / s2);
      // B
      min_b = s3 == 0.0 ? kPredB + 1 : 0;
      max_b = s3 == 0.0 ? min_b + 10 : static_cast<int64_t>(kPredB / s3);
      // C
      min_c = s1 == 0.0 ? kPredC + 1 : 0;
      max_c = s1 == 0.0 ? min_c + 10 : static_cast<int64_t>(kPredC / s1);
    } else {
      // A
      min_a = s3 == 0.0 ? kPredA + 1 : 0;
      max_a = s3 == 0.0 ? min_a + 10 : static_cast<int64_t>(kPredA / s3);
      // B
      min_b = s1 == 0.0 ? kPredB + 1 : 0;
      max_b = s1 == 0.0 ? min_b + 10 : static_cast<int64_t>(kPredB / s1);
      // C
      min_c = s2 == 0.0 ? kPredC + 1 : 0;
      max_c = s2 == 0.0 ? min_c + 10 : static_cast<int64_t>(kPredC / s2);
    }

    LOG_INFO("Phase {}: A=[{},{}], B=[{},{}], C=[{},{}]", i, min_a, max_a, min_b, max_b, min_c,
             max_c);

    // Insert batches for this phase.
    for (uint32_t j = 0; j < num_batches_per_phase; j++) {
      std::vector<sql::ColumnSegment> columns;
      auto *as = reinterpret_cast<byte *>(GenColumnData(batch_size, min_a, max_a));
      auto *bs = reinterpret_cast<byte *>(GenColumnData(batch_size, min_b, max_b));
      auto *cs = reinterpret_cast<byte *>(GenColumnData(batch_size, min_c, max_c));

      columns.emplace_back(sql::IntegerType::Instance(false), as, nullptr, batch_size);
      columns.emplace_back(sql::IntegerType::Instance(false), bs, nullptr, batch_size);
      columns.emplace_back(sql::IntegerType::Instance(false), cs, nullptr, batch_size);

      // Insert into table
      table->Insert(sql::Table::Block(std::move(columns), batch_size));
    }
  }
}

// Create all test tables. One per overall selectivity.
void InitTestTables() {
  sql::Catalog *catalog = sql::Catalog::Instance();
  for (const auto &table_meta : kTables) {
    // Create the table instance.
    std::vector<sql::Schema::ColumnInfo> cols = {{"A", sql::IntegerType::Instance(false)},
                                                 {"B", sql::IntegerType::Instance(false)},
                                                 {"C", sql::IntegerType::Instance(false)}};
    auto table = std::make_unique<sql::Table>(catalog->AllocateTableId(), table_meta.name,
                                              std::make_unique<sql::Schema>(std::move(cols)));

    // Populate it.
    const auto population_time = util::Time<std::milli>([&]() {
      auto [s1, s2, s3] = table_meta.col_selectivities;
      if (!util::MathUtil::ApproxEqual(table_meta.selectivity, s1 * s2 * s3)) {
        throw NotImplementedException("Computed selectivity [{}] doesn't match expected [{}].",
                                      s1 * s2 * s3, table_meta.selectivity);
      }
      LoadTestTable(table.get(), kNumTableRows, s1, s2, s3);
    });
    LOG_INFO("Populated table {} ({} ms)", table_meta.name, population_time);

    // Insert into catalog.
    catalog->InsertTable(table_meta.name, std::move(table));
  }
}

// Retrieve a test table that has the provided overall selectivity for the
// predicate. NULL if no such table exists.
const sql::Table *GetTestTable(double selectivity) {
  uint32_t lower = 0, upper = kNumTables - 1;
  while (lower != upper) {
    uint32_t middle = lower + ((upper - lower) / 2);
    if (kTables[middle].selectivity < selectivity) {
      lower = middle + 1;
    } else if (kTables[middle].selectivity > selectivity) {
      upper = middle;
    } else {
      return sql::Catalog::Instance()->LookupTableByName(kTables[middle].name);
    }
  }
  return nullptr;
}

// Run the filter manager experiment a fixed number of times and take the average.
// Using the provided filter terms and adaptation flag.
void RunScanWithTermOrder(uint16_t table_id, bool adapt,
                          const std::vector<sql::FilterManager::MatchFn> &terms,
                          std::vector<double> *results) {
  for (uint32_t r = 0; r < kNumRuns; r++) {
    std::vector<double> run_results;
    run_results.reserve(2000);

    sql::FilterManager filter(adapt);
    filter.StartNewClause();
    filter.InsertClauseTerms(terms);

    sql::TableVectorIterator tvi(static_cast<uint16_t>(table_id));
    for (tvi.Init(); tvi.Advance();) {
      auto vpi = tvi.GetVectorProjectionIterator();
      const auto exec_micros = util::Time<std::micro>([&] { filter.RunFilters(vpi); });
      run_results.push_back(exec_micros);
    }

    if (r == 0) {
      *results = run_results;
    } else {
      for (uint32_t i = 0; i < run_results.size(); i++) {
        (*results)[i] += run_results[i];
      }
    }
  }

  // Average out.
  for (std::size_t i = 0; i < results->size(); i++) {
    (*results)[i] /= static_cast<double>(kNumRuns);
  }
}

void RunExperiment(uint16_t table_id, std::vector<std::vector<double>> *all_results,
                   std::vector<double> *adapt_results) {
  // Perform experiment on all possible orderings.
  std::vector<uint32_t> term_order = {0, 1, 2};
  do {
    std::vector<double> timings;
    RunScanWithTermOrder(table_id, false, GetTermsByOrder(term_order), &timings);
    all_results->emplace_back(std::move(timings));
  } while (std::next_permutation(term_order.begin(), term_order.end()));

  // Run adaptive mode.
  RunScanWithTermOrder(table_id, true, GetTermsByOrder({0, 1, 2}), adapt_results);
}

}  // namespace

class FilterManagerBenchmark : public benchmark::Fixture {
 public:
  void SetUp(benchmark::State &st) override {
    // Let base setup.
    Fixture::SetUp(st);
    // Now us.
    static std::once_flag init_flag;
    std::call_once(init_flag, []() {
      // Create tables.
      InitTestTables();
      // Disabled some optimizations.
      Settings::Instance()->SetDouble(Settings::Name::SelectOptThreshold, 7.00);
    });
  }
};

BENCHMARK_DEFINE_F(FilterManagerBenchmark, TimeSeries)(benchmark::State &state) {
  auto table_id = GetTestTable(0.02)->GetId();

  std::vector<std::vector<double>> results;  // Subset of interesting results.
  std::vector<double> adapt_results;         // Results from adaptive filter.

  {
    // 'all_results' holds, for each possible ordering, the timings for each
    // partition. We time and capture all orderings because we don't know
    // which will be best ahead of time. So, we'll run them all, sort them
    // and choose the ones we want.
    std::vector<std::vector<double>> all_results;
    RunExperiment(table_id, &all_results, &adapt_results);

    // Sort the results by time. 'all_results[0]' will be best.
    std::sort(all_results.begin(), all_results.end(), [](const auto &a, const auto &b) {
      auto a_total = std::accumulate(a.begin(), a.end(), 0.0);
      auto b_total = std::accumulate(b.begin(), b.end(), 0.0);
      return a_total <= b_total;
    });

    const auto num_orders = all_results.size();

    // Collect a subset of all the order timings.
    results.push_back(all_results[0]);                   // Best
    results.push_back(all_results[num_orders / 2]);      // 50%
    results.push_back(all_results[3 * num_orders / 4]);  // 75%
    results.push_back(all_results.back());               // Worst
  }

  // Print results.
  const auto num_orders = results.size();
  const auto num_parts = results[0].size();

  // Print CSV of per-partition timings.
  std::ofstream out_file;
  out_file.open("filter.csv");

  out_file << "Partition, ";
  for (uint32_t order = 0; order < num_orders; order++) {
    out_file << "Order_" << order << ", ";
  }
  out_file << "Adaptive" << std::endl;

  for (uint32_t i = 0; i < num_parts; i++) {
    out_file << i << ", ";
    out_file << std::fixed << std::setprecision(2);
    for (uint32_t j = 0; j < num_orders; j++) {
      out_file << results[j][i] << ", ";
    }
    out_file << adapt_results[i] << std::endl;
  }
}

BENCHMARK_DEFINE_F(FilterManagerBenchmark, VaryPredicateSelectivity)(benchmark::State &state) {
  std::ofstream out_file("filter-vary-selectivity.csv");
  for (const auto &table : kTables) {
    const auto table_id = sql::Catalog::Instance()->LookupTableByName(table.name)->GetId();

    std::vector<std::vector<double>> results;
    std::vector<double> adapt_results;
    RunExperiment(table_id, &results, &adapt_results);

    // Sort the results by time. 'all_results[0]' will be best.
    std::sort(results.begin(), results.end(), [](const auto &a, const auto &b) {
      auto a_total = std::accumulate(a.begin(), a.end(), 0.0);
      auto b_total = std::accumulate(b.begin(), b.end(), 0.0);
      return a_total <= b_total;
    });

    const auto total_best = std::accumulate(results[0].begin(), results[0].end(), 0.0);
    const auto total_worst = std::accumulate(results.back().begin(), results.back().end(), 0.0);
    const auto total_adaptive = std::accumulate(adapt_results.begin(), adapt_results.end(), 0.0);
    out_file << std::fixed << std::setprecision(2) << table.selectivity << ", ";
    out_file << total_worst << ", " << total_best << ", " << total_adaptive << std::endl;
  }
}

BENCHMARK_DEFINE_F(FilterManagerBenchmark, VarySamplingRate)(benchmark::State &state) {
  const auto table_id = GetTestTable(0.02)->GetId();

  // Total time per sampling-rate.
  std::vector<double> results;
  // One result for each sampling rate.
  std::vector<double> sample_freqs = {0.00, 0.10, 0.20, 0.30, 0.40, 0.50,
                                      0.60, 0.70, 0.80, 0.90, 1.00};
  // Sampling rates.
  for (double sample_freq : sample_freqs) {
    // Set rate.
    Settings::Instance()->SetDouble(Settings::Name::AdaptivePredicateOrderSamplingFrequency,
                                    sample_freq);
    // Run experiment.
    std::vector<double> sample_results;
    RunScanWithTermOrder(table_id, true, GetTermsByOrder({0, 1, 2}), &sample_results);
    results.push_back(std::accumulate(sample_results.begin(), sample_results.end(), 0.0));
  }

  // Write out.
  std::ofstream out_file("filter-vary-sampling.csv");
  for (uint32_t i = 0; i < results.size(); i++) {
    out_file << std::fixed << std::setprecision(2);
    out_file << sample_freqs[i] << ", " << results[i] << std::endl;
  }
  out_file.close();
}

// ---------------------------------------------------------
//
// Benchmark Configs
//
// ---------------------------------------------------------

BENCHMARK_REGISTER_F(FilterManagerBenchmark, TimeSeries);
BENCHMARK_REGISTER_F(FilterManagerBenchmark, VaryPredicateSelectivity);
BENCHMARK_REGISTER_F(FilterManagerBenchmark, VarySamplingRate);

}  // namespace tpl

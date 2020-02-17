#include <ostream>
#include "sql/planner/plannodes/output_schema.h"
#include "sql/schema.h"
#include "sql/value.h"
#include "util/test_harness.h"

// TODO(Amadou): Currently all checker only work on single integer columns. Ideally, we want them to
// work on arbitrary expressions, but this is no simple task. We would basically need an expression
// evaluator on output rows.

namespace tpl::sql::codegen {
/**
 * Helper class to check if the output of a query is corrected.
 */
class OutputChecker {
 public:
  virtual void CheckCorrectness() = 0;
  virtual void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) = 0;
};

/**
 * Runs multiples output checkers at once
 */
class MultiChecker : public OutputChecker {
 public:
  /**
   * Constructor
   * @param checkers list of output checkers
   */
  MultiChecker(std::vector<OutputChecker *> &&checkers) : checkers_{std::move(checkers)} {}

  /**
   * Call checkCorrectness on all output checkers
   */
  void CheckCorrectness() override {
    for (const auto checker : checkers_) {
      checker->CheckCorrectness();
    }
  }

  /**
   * Calls all output checkers
   * @param output current output batch
   */
  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    for (const auto checker : checkers_) {
      checker->ProcessBatch(output);
    }
  }

 private:
  // list of checkers
  std::vector<OutputChecker *> checkers_;
};

using RowChecker = std::function<void(const std::vector<const sql::Val *> &)>;
using CorrectnessFn = std::function<void()>;
class GenericChecker : public OutputChecker {
 public:
  GenericChecker(RowChecker row_checker, CorrectnessFn correctness_fn)
      : row_checker_{row_checker}, correctness_fn_(correctness_fn) {}

  void CheckCorrectness() override {
    if (bool(correctness_fn_)) correctness_fn_();
  }

  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    if (!bool(row_checker_)) return;
    for (const auto &vals : output) {
      row_checker_(vals);
    }
  }

 private:
  RowChecker row_checker_;
  CorrectnessFn correctness_fn_;
};

/**
 * Checks if the number of output tuples is correct
 */
class NumChecker : public OutputChecker {
 public:
  /**
   * Constructor
   * @param expected_count the expected number of output tuples
   */
  NumChecker(int64_t expected_count) : expected_count_{expected_count} {}

  /**
   * Checks if the expected number and the received number are the same
   */
  void CheckCorrectness() override { EXPECT_EQ(curr_count_, expected_count_); }

  /**
   * Increment the current count
   * @param output current output batch
   */
  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    curr_count_ += output.size();
  }

 private:
  // Current number of tuples
  int64_t curr_count_{0};
  // Expected number of tuples
  int64_t expected_count_;
};

/**
 * Checks that the values in a column satisfy a certain comparison.
 * @tparam CompFn comparison function to use
 */
class SingleIntComparisonChecker : public OutputChecker {
 public:
  SingleIntComparisonChecker(std::function<bool(int64_t, int64_t)> fn, uint32_t col_idx,
                             int64_t rhs)
      : comp_fn_(fn), col_idx_{col_idx}, rhs_{rhs} {}

  void CheckCorrectness() override {}

  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    for (const auto &vals : output) {
      auto int_val = static_cast<const sql::Integer *>(vals[col_idx_]);
      EXPECT_TRUE(comp_fn_(int_val->val, rhs_));
    }
  }

 private:
  std::function<bool(int64_t, int64_t)> comp_fn_;
  uint32_t col_idx_;
  int64_t rhs_;
};

/**
 * Checks if two joined columns are the same.
 */
class SingleIntJoinChecker : public OutputChecker {
 public:
  /**
   * Constructor
   * @param col1 first column of the join
   * @param col2 second column of the join
   */
  SingleIntJoinChecker(uint32_t col1, uint32_t col2) : col1_(col1), col2_(col2) {}

  /**
   * Does nothing. All the checks are done in ProcessBatch
   */
  void CheckCorrectness() override {}

  /**
   * Checks that the two joined columns are the same
   * @param output current output
   */
  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    for (const auto &vals : output) {
      auto val1 = static_cast<const sql::Integer *>(vals[col1_]);
      auto val2 = static_cast<const sql::Integer *>(vals[col2_]);
      EXPECT_EQ(val1->val, val2->val);
    }
  }

 private:
  uint32_t col1_;
  uint32_t col2_;
};

/**
 * Checks that a columns sums up to an expected value
 */
class SingleIntSumChecker : public OutputChecker {
 public:
  /**
   * Constructor
   * @param col_idx index of column to sum
   * @param expected expected sum
   */
  SingleIntSumChecker(uint32_t col_idx, int64_t expected)
      : col_idx_{col_idx}, expected_{expected} {}

  /**
   * Checks of the expected sum and the received sum are the same
   */
  void CheckCorrectness() override { EXPECT_EQ(curr_sum_, expected_); }

  /**
   * Update the current sum
   * @param output current output batch
   */
  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    for (const auto &vals : output) {
      auto int_val = static_cast<const sql::Integer *>(vals[col_idx_]);
      if (!int_val->is_null) curr_sum_ += int_val->val;
    }
  }

 private:
  uint32_t col_idx_;
  int64_t curr_sum_{0};
  int64_t expected_;
};

/**
 * Checks that a given column is sorted
 */
class SingleIntSortChecker : public OutputChecker {
 public:
  /**
   * Constructor
   * @param col_idx column to check
   */
  SingleIntSortChecker(uint32_t col_idx) : col_idx_{0} {}

  /**
   * Does nothing. All the checking is done in ProcessBatch.
   */
  void CheckCorrectness() override {}

  /**
   * Compares each value with the previous one to make sure they are sorted
   * @param output current output batch.
   */
  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    for (const auto &vals : output) {
      auto int_val = static_cast<const sql::Integer *>(vals[col_idx_]);
      if (int_val->is_null) {
        EXPECT_TRUE(prev_val.is_null);
      } else {
        EXPECT_TRUE(prev_val.is_null || int_val->val >= prev_val.val);
      }
      // Copy the value since the pointer does not belong to this class.
      prev_val = *int_val;
    }
  }

 private:
  sql::Integer prev_val{sql::Integer::Null()};
  uint32_t col_idx_;
};

/**
 * Runs multiple OutputCallbacks at once
 */
class MultiOutputCallback : public sql::ResultConsumer {
 public:
  /**
   * Constructor
   * @param callbacks list of output callbacks
   */
  MultiOutputCallback(std::vector<sql::ResultConsumer *> callbacks)
      : callbacks_{std::move(callbacks)} {}

  /**
   * OutputCallback function
   */
  void Consume(const sql::OutputBuffer &tuples) {
    for (auto &callback : callbacks_) {
      callback->Consume(tuples);
    }
  }

 private:
  std::vector<sql::ResultConsumer *> callbacks_;
};

/**
 * Consumer that prints out results to the given output stream.
 */
class OutputPrinter : public sql::ResultConsumer {
 public:
  /**
   * Create a new consumer.
   * @param os The stream to write the results into.
   * @param output_schema The schema of the output of the query.
   */
  OutputPrinter(const sql::planner::OutputSchema *output_schema) : output_schema_(output_schema) {}

  /**
   * Print out the tuples in the input batch.
   * @param batch The batch of result tuples to print.
   */
  void Consume(const OutputBuffer &batch) override {
    for (const byte *raw_tuple : batch) {
      PrintTuple(raw_tuple);
    }
  }

 private:
  // Print one tuple
  void PrintTuple(const byte *tuple) const {
    bool first = true;
    for (const auto &col_info : output_schema_->GetColumns()) {
      // Comma
      if (!first) std::cout << ",";
      first = false;

      // Column value
      switch (GetSqlTypeFromInternalType(col_info.GetType())) {
        case SqlTypeId::Boolean: {
          const auto val = reinterpret_cast<const BoolVal *>(tuple);
          std::cout << (val->is_null ? "NULL" : val->val ? "True" : "False");
          tuple += sizeof(BoolVal);
          break;
        }
        case SqlTypeId::TinyInt:
        case SqlTypeId::SmallInt:
        case SqlTypeId::Integer:
        case SqlTypeId::BigInt: {
          const auto val = reinterpret_cast<const Integer *>(tuple);
          std::cout << (val->is_null ? "NULL" : std::to_string(val->val));
          tuple += sizeof(Integer);
          break;
        }
        case SqlTypeId::Real:
        case SqlTypeId::Double:
        case SqlTypeId::Decimal: {
          const auto val = reinterpret_cast<const Real *>(tuple);
          if (val->is_null) {
            std::cout << "NULL";
          } else {
            std::cout << std::fixed << std::setprecision(2) << val->val << std::dec;
          }
          tuple += sizeof(Real);
          break;
        }
        case SqlTypeId::Date: {
          const auto val = reinterpret_cast<const DateVal *>(tuple);
          std::cout << (val->is_null ? "NULL" : val->val.ToString());
          tuple += sizeof(DateVal);
          break;
        }
        case SqlTypeId::Char:
        case SqlTypeId::Varchar: {
          const auto val = reinterpret_cast<const StringVal *>(tuple);
          if (val->is_null) {
            std::cout << "NULL";
          } else {
            std::cout << "'" << val->val.GetStringView() << "'";
          }
          tuple += sizeof(StringVal);
          break;
        }
      }
    }

    // New line
    std::cout << std::endl;
  }

 private:
  // The output schema
  const sql::planner::OutputSchema *output_schema_;
};

/**
 * An output callback that stores the rows in a vector and runs a checker on them.
 */
class OutputStore : public sql::ResultConsumer {
 public:
  /**
   * Constructor
   * @param checker checker to run
   * @param schema output schema of the query.
   */
  OutputStore(OutputChecker *checker, const planner::OutputSchema *schema)
      : schema_(schema), checker_(checker) {}

  /**
   * OutputCallback function. This will gather the output in a vector.
   */
  void Consume(const sql::OutputBuffer &tuples) override {
    for (uint32_t row = 0; row < tuples.size(); row++) {
      uint32_t curr_offset = 0;
      std::vector<const sql::Val *> vals;
      for (uint16_t col = 0; col < schema_->GetColumns().size(); col++) {
        // TODO(Amadou): Figure out to print other types.
        switch (schema_->GetColumn(col).GetType()) {
          case sql::TypeId::TinyInt:
          case sql::TypeId::SmallInt:
          case sql::TypeId::BigInt:
          case sql::TypeId::Integer: {
            auto *val = reinterpret_cast<const sql::Integer *>(tuples[row] + curr_offset);
            vals.emplace_back(val);
            curr_offset += sizeof(sql::Integer);
            break;
          }
          case sql::TypeId::Boolean: {
            auto *val = reinterpret_cast<const sql::BoolVal *>(tuples[row] + curr_offset);
            vals.emplace_back(val);
            curr_offset += sizeof(sql::BoolVal);
            break;
          }
          case sql::TypeId::Float:
          case sql::TypeId::Double: {
            auto *val = reinterpret_cast<const sql::Real *>(tuples[row] + curr_offset);
            vals.emplace_back(val);
            curr_offset += sizeof(sql::Real);
            break;
          }
          case sql::TypeId::Date: {
            auto *val = reinterpret_cast<const sql::DateVal *>(tuples[row] + curr_offset);
            vals.emplace_back(val);
            curr_offset += sizeof(sql::DateVal);
            break;
          }
          case sql::TypeId::Varchar: {
            auto *val = reinterpret_cast<const sql::StringVal *>(tuples[row] + curr_offset);
            vals.emplace_back(val);
            curr_offset += sizeof(sql::StringVal);
            break;
          }
          default:
            UNREACHABLE("Cannot output unsupported type!!!");
        }
      }
      output.emplace_back(vals);
    }
    checker_->ProcessBatch(output);
    output.clear();
  }

 private:
  // Current output batch
  std::vector<std::vector<const sql::Val *>> output;
  // output schema
  const planner::OutputSchema *schema_;
  // checker to run
  OutputChecker *checker_;
};
}  // namespace tpl::sql::codegen

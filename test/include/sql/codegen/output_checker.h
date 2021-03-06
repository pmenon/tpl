#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "sql/planner/plannodes/output_schema.h"
#include "sql/result_consumer.h"
#include "sql/schema.h"
#include "sql/value.h"
#include "util/test_harness.h"

namespace tpl::sql::codegen {

/**
 * Helper class to check if the output of a query is correct.
 */
class OutputChecker {
 public:
  /**
   * Default base.
   */
  virtual ~OutputChecker() = default;

  /**
   * Called after the query has completed to check the results in total.
   */
  virtual void CheckCorrectness() = 0;

  /**
   * Called on a per-output-batch basis.
   * @param output A batch of output in row-form.
   */
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
  explicit MultiChecker(std::vector<std::unique_ptr<OutputChecker>> &&args)
      : checkers_(std::move(args)) {}

  /**
   * Call checkCorrectness on all output checkers
   */
  void CheckCorrectness() override {
    for (const auto &checker : checkers_) {
      checker->CheckCorrectness();
    }
  }

  /**
   * Calls all output checkers
   * @param output current output batch
   */
  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    for (const auto &checker : checkers_) {
      checker->ProcessBatch(output);
    }
  }

 private:
  std::vector<std::unique_ptr<OutputChecker>> checkers_;
};

using RowChecker = std::function<void(const std::vector<const sql::Val *> &)>;
using CorrectnessFn = std::function<void()>;
class GenericChecker : public OutputChecker {
 public:
  GenericChecker(RowChecker row_checker, CorrectnessFn correctness_fn)
      : row_checker_(std::move(row_checker)), correctness_fn_(std::move(correctness_fn)) {}

  void CheckCorrectness() override {
    if (correctness_fn_) correctness_fn_();
  }

  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    if (!row_checker_) return;
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
class TupleCounterChecker : public OutputChecker {
 public:
  /**
   * Constructor.
   * @param expected_count the expected number of output tuples
   */
  explicit TupleCounterChecker(int64_t expected_count)
      : curr_count_(0), expected_count_(expected_count) {}

  /**
   * Checks if the expected number and the received number are the same.
   */
  void CheckCorrectness() override { EXPECT_EQ(curr_count_, expected_count_); }

  /**
   * Increment the current count.
   * @param output current output batch.
   */
  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    curr_count_ += output.size();
  }

 private:
  // Current number of tuples
  int64_t curr_count_;
  // Expected number of tuples
  int64_t expected_count_;
};

/**
 * Checks that the values in a column satisfy a certain comparison for each output row.
 * @tparam CompFn comparison function to use
 */
template <typename T>
class SingleColumnValueChecker : public OutputChecker {
  static_assert(std::is_base_of_v<sql::Val, T>, "Template type must be SQL value");

  // The underlying CPP type.
  using CppType = decltype(T::val);

 public:
  SingleColumnValueChecker(std::function<bool(CppType, CppType)> fn, uint32_t col_idx, CppType rhs)
      : comp_fn_(std::move(fn)), col_idx_(col_idx), rhs_(rhs) {}

  void CheckCorrectness() override {}

  void ProcessBatch(const std::vector<std::vector<const sql::Val *>> &output) override {
    for (const auto &vals : output) {
      auto val = static_cast<const T *>(vals[col_idx_]);
      EXPECT_TRUE(comp_fn_(val->val, rhs_)) << "lhs=" << val->val << ",rhs=" << rhs_;
    }
  }

 private:
  std::function<bool(CppType, CppType)> comp_fn_;
  uint32_t col_idx_;
  CppType rhs_;
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
      ASSERT_EQ(val1->val, val2->val);
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
      : col_idx_(col_idx), curr_sum_(0), expected_(expected) {}

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
  int64_t curr_sum_;
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
  explicit SingleIntSortChecker(uint32_t col_idx)
      : col_idx_(col_idx), prev_val_(sql::Integer::Null()) {}

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
        EXPECT_TRUE(prev_val_.is_null);
      } else {
        EXPECT_TRUE(prev_val_.is_null || int_val->val >= prev_val_.val)
            << prev_val_.val << " NOT < " << int_val->val;
      }
      prev_val_ = *int_val;
    }
  }

 private:
  uint32_t col_idx_;
  sql::Integer prev_val_;
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
  explicit MultiOutputCallback(std::vector<sql::ResultConsumer *> callbacks)
      : callbacks_(std::move(callbacks)) {}

  /**
   * OutputCallback function
   */
  void Consume(const sql::OutputBuffer &tuples) override {
    for (auto &callback : callbacks_) {
      callback->Consume(tuples);
    }
  }

 private:
  std::vector<sql::ResultConsumer *> callbacks_;
};

/**
 * An output callback that stores the rows in a vector and runs a checker on them.
 */
class OutputCollectorAndChecker : public sql::ResultConsumer {
 public:
  /**
   * Constructor
   * @param checker The checks to run on each output batch from the query.
   * @param schema The output schema of the query.
   */
  OutputCollectorAndChecker(OutputChecker *checker, const planner::OutputSchema *schema)
      : schema_(schema), offsets_(schema->GetColumnOffsets()), checker_(checker) {}

  /**
   * OutputCallback function. This will gather the output in a vector.
   */
  void Consume(const sql::OutputBuffer &tuples) override {
    std::vector<std::vector<const sql::Val *>> output;
    for (uint32_t row_idx = 0; row_idx < tuples.size(); row_idx++) {
      std::vector<const sql::Val *> row(schema_->NumColumns());
      for (uint32_t col_idx = 0; col_idx < schema_->NumColumns(); col_idx++) {
        row[col_idx] = reinterpret_cast<const sql::Val *>(tuples[row_idx] + offsets_[col_idx]);
      }
      output.emplace_back(row);
    }

    // Send along.
    checker_->ProcessBatch(output);
  }

 private:
  // Output schema.
  const planner::OutputSchema *schema_;
  // Byte offsets of each column in the output.
  std::vector<std::size_t> offsets_;
  // Checker to run.
  OutputChecker *checker_;
};

}  // namespace tpl::sql::codegen

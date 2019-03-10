#pragma once

#include "sql/value.h"
#include "util/common.h"
#include "util/macros.h"

namespace tpl::sql {

// ---------------------------------------------------------
// Count
// ---------------------------------------------------------

class CountAggregator {
 public:
  /// Construct
  CountAggregator() : count_(0) {}

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(CountAggregator);

  /// Advance the count based on the NULLness of the input value
  void Advance(const Val *val) { count_ += !val->null; }

  /// Merge this count with the \a that count
  void Merge(const CountAggregator &that) { count_ += that.count_; }

  /// Reset the aggregate
  void Reset() noexcept { count_ = 0; }

  /// Return the current value of the count
  Integer GetCountResult() const {
    return Integer(false, static_cast<i64>(count_));
  }

 private:
  u64 count_;
};

// ---------------------------------------------------------
// Count Star
// ---------------------------------------------------------

class CountStarAggregate {
 public:
  /// Construct
  CountStarAggregate() : count_(0) {}

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(CountStarAggregate);

  /// Advance the aggregate by one
  void Advance(UNUSED const Val *val) { count_++; }

  /// Merge this count with the \a that count
  void Merge(const CountStarAggregate &that) { count_ += that.count_; }

  /// Reset the aggregate
  void Reset() noexcept { count_ = 0; }

  /// Return the current value of the count
  Integer GetCountResult() const {
    return Integer(false, static_cast<i64>(count_));
  }

 private:
  u64 count_;
};

// ---------------------------------------------------------
// Sums
// ---------------------------------------------------------

/// Base class for Sums
class SumAggregate {
 public:
  /// Construct
  SumAggregate() : num_updates_(0) {}

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(SumAggregate);

  /// Increment the number of tuples this aggregate has seen
  void IncrementUpdateCount() { num_updates_++; }

  /// Reset
  void ResetUpdateCount() { num_updates_ = 0; }

  /// Merge this sum with the one provided
  void Merge(const SumAggregate &that) { num_updates_ += that.num_updates_; }

  u64 GetNumUpdates() const { return num_updates_; }

 private:
  u64 num_updates_;
};

/// Integer Sums
class IntegerSumAggregate : public SumAggregate {
 public:
  /// Constructor
  IntegerSumAggregate() : SumAggregate(), sum_(0) {}

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(IntegerSumAggregate);

  /// Advance the aggregate by the input value \a val
  template <bool Nullable>
  void Advance(const Integer *val);

  /// Reset the aggregate
  void Reset() noexcept {
    ResetUpdateCount();
    sum_ = 0;
  }

  /// Return the result of the summation
  Integer GetResultSum() const { return Integer(GetNumUpdates() == 0, sum_); }

 private:
  i64 sum_;
};

template <>
void IntegerSumAggregate::Advance<true>(const Integer *val) {
  if (!val->null) {
    IncrementUpdateCount();
    sum_ += val->val.integer;
  }
}

template <>
void IntegerSumAggregate::Advance<false>(const Integer *val) {
  TPL_ASSERT(val->null, "Received NULL input in non-NULLable aggregator!");
  IncrementUpdateCount();
  sum_ += val->val.integer;
}

}  // namespace tpl::sql
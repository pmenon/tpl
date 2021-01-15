#pragma once

#include <algorithm>
#include <limits>

#include "common/common.h"
#include "common/macros.h"
#include "sql/value.h"

namespace tpl::sql {

/**
 * Base class for COUNT() aggregates. If the template parameter is true, this aggregator only counts
 * non-NULL SQL values. If the template parameter is false, this aggregator will count all inputs.
 * @tparam SkipNulls Boolean template flag controlling whether NULL SQL inputs are counted.
 */
template <bool SkipNulls, typename T = uint64_t>
class CountAggregateBase {
 public:
  static_assert(!std::is_same_v<T, bool> && std::is_integral_v<T> && std::is_unsigned_v<T>,
                "Invalid count type.");

  /**
   * Constructor.
   */
  CountAggregateBase() : count_(0) {}

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(CountAggregateBase);

  /**
   * Advance the count based on the NULL-ness of the input value.
   */
  void Advance(const Val &val) { count_ += (SkipNulls ? !val.is_null : T{1}); }

  /**
   * Merge this count with the @em that count.
   */
  void Merge(const CountAggregateBase &that) { count_ += that.count_; }

  /**
   * Reset the aggregate.
   */
  void Reset() { count_ = T{0}; }

  /**
   * Return the current value of the count.
   */
  Integer GetCountResult() const { return Integer(count_); }

 private:
  T count_;
};

/**
 * A COUNT(col) aggregate. Only counts non-NULL inputs.
 */
class CountAggregate : public CountAggregateBase<true> {};

/**
 * A COUNT(*) aggregate. Counts all inputs.
 */
class CountStarAggregate : public CountAggregateBase<false> {};

/**
 * Generic summations.
 */
template <typename T>
class SumAggregate {
  static_assert(std::is_base_of_v<Val, T>, "Type must be a SQL value, i.e., derive from sql::Val");

 public:
  /**
   * Constructor.
   */
  SumAggregate() : sum_(static_cast<decltype(T::val)>(0)) { sum_.is_null = true; }

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(SumAggregate);

  /**
   * Advance the aggregate by a given input value. If the input is NULL, no
   * change is applied to the aggregate.
   * @param val The (potentially NULL) value to advance the sum by.
   */
  void Advance(const T &val) {
    if (val.is_null) return;
    sum_.is_null = false;
    sum_.val += val.val;
  }

  /**
   * Merge a partial sum aggregate into this aggregate. If the partial sum is
   * NULL, no change is applied to this aggregate.
   * @param that The (potentially NULL) value to merge into this aggregate.
   */
  void Merge(const SumAggregate<T> &that) {
    if (that.sum_.is_null) {
      return;
    }
    sum_.is_null = false;
    sum_.val += that.sum_.val;
  }

  /**
   * Reset the summation.
   */
  void Reset() {
    sum_.is_null = true;
    sum_.val = 0;
  }

  /**
   * Return the result of the summation.
   * @return The current value of the sum.
   */
  const T &GetResultSum() const { return sum_; }

 private:
  T sum_;
};

/**
 * Integer Sums
 */
class IntegerSumAggregate : public SumAggregate<Integer> {};

/**
 * Real Sums
 */
class RealSumAggregate : public SumAggregate<Real> {};

/**
 * Generic max.
 */
template <typename T>
class MaxAggregate {
  static_assert(std::is_base_of_v<Val, T>, "Template type must subclass value");

 public:
  /**
   * Constructor.
   */
  MaxAggregate() : max_(std::numeric_limits<decltype(T::val)>::min()) { max_.is_null = true; }

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(MaxAggregate);

  /**
   * Advance the aggregate by the input value @em val.
   */
  void Advance(const T &val) {
    if (val.is_null) return;
    max_.is_null = false;
    max_.val = std::max(val.val, max_.val);
  }

  /**
   * Merge a partial max aggregate into this aggregate.
   */
  void Merge(const MaxAggregate<T> &that) {
    if (that.max_.is_null) {
      return;
    }
    max_.is_null = false;
    max_.val = std::max(that.max_.val, max_.val);
  }

  /**
   * Reset the aggregate.
   */
  void Reset() {
    max_.is_null = true;
    max_.val = std::numeric_limits<decltype(T::val)>::min();
  }

  /**
   * Return the result of the max.
   */
  const T &GetResultMax() const { return max_; }

 private:
  T max_;
};

/**
 * Integer Max
 */
class IntegerMaxAggregate : public MaxAggregate<Integer> {};

/**
 * Real Max
 */
class RealMaxAggregate : public MaxAggregate<Real> {};

/**
 * Date Max
 */
class DateMaxAggregate : public MaxAggregate<DateVal> {};

/**
 * Timestamp Max
 */
class TimestampMaxAggregate : public MaxAggregate<TimestampVal> {};

/**
 * String Max
 */
class StringMaxAggregate : public MaxAggregate<StringVal> {};

/**
 * Generic min.
 */
template <typename T>
class MinAggregate {
  static_assert(std::is_base_of_v<Val, T>, "Template type must subclass value");

 public:
  /**
   * Constructor.
   */
  MinAggregate() : min_(std::numeric_limits<decltype(T::val)>::max()) { min_.is_null = true; }

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(MinAggregate);

  /**
   * Advance the aggregate by the input value @em val.
   */
  void Advance(const T &val) {
    if (val.is_null) return;
    min_.is_null = false;
    min_.val = std::min(val.val, min_.val);
  }

  /**
   * Merge a partial min aggregate into this aggregate.
   */
  void Merge(const MinAggregate<T> &that) {
    if (that.min_.is_null) {
      return;
    }
    min_.is_null = false;
    min_.val = std::min(that.min_.val, min_.val);
  }

  /**
   * Reset the aggregate.
   */
  void Reset() {
    min_.is_null = true;
    min_.val = std::numeric_limits<decltype(T::val)>::max();
  }

  /**
   * Return the result of the minimum.
   */
  const T &GetResultMin() const { return min_; }

 private:
  T min_;
};

/**
 * Integer Min
 */
class IntegerMinAggregate : public MinAggregate<Integer> {};

/**
 * Real Min
 */
class RealMinAggregate : public MinAggregate<Real> {};

/**
 * Date Min
 */
class DateMinAggregate : public MinAggregate<DateVal> {};

/**
 * Timestamp Min
 */
class TimestampMinAggregate : public MinAggregate<TimestampVal> {};

/**
 * String Min
 */
class StringMinAggregate : public MinAggregate<StringVal> {};

/**
 * Average aggregate.
 */
class AvgAggregate {
 public:
  /**
   * Constructor.
   */
  AvgAggregate() : sum_(0.0), count_(0) {}

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(AvgAggregate);

  /**
   * Advance the aggregate by the input value @em val.
   */
  template <typename T>
  void Advance(const T &val) {
    if (val.is_null) return;
    sum_ += val.val;
    count_++;
  }

  /**
   * Merge a partial average aggregate into this aggregate.
   */
  void Merge(const AvgAggregate &that) {
    sum_ += that.sum_;
    count_ += that.count_;
  }

  /**
   * Reset the aggregate.
   */
  void Reset() {
    sum_ = 0.0;
    count_ = 0;
  }

  /**
   * Return the result of the minimum.
   */
  Real GetResultAvg() const {
    if (count_ == 0) {
      return Real::Null();
    }
    return Real(sum_ / count_);
  }

 private:
  double sum_;
  uint64_t count_;
};

}  // namespace tpl::sql

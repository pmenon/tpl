//
//  zipfian_generator.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/7/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//
//  pmenon 3/18/20: Cleaned up for TPL.

#pragma once

#include <limits>
#include <mutex>  // NOLINT

namespace tpl::util {

/**
 * A generator that produces a sequence of elements according to a Zipfian distribution. When
 * constructing an instance, you specify either a total item count (in which case values in the
 * range [0, item_count] inclusive are produced), or specify a minimum and maximum value (in which
 * case values in [min,max] inclusive are produced). You can also specify a skew constant that
 * controls the degree to which elements are skewed towards the popular elements. A higher skew
 * values favors popular elements.
 *
 * Popular elements are clustered together: min is the most popular, min+1 the second most popular,
 * etc.
 *
 * The algorithm used here is from "Quickly Generating Billion-Record Synthetic Databases",
 * Jim Gray et al, SIGMOD 1994.
 */
class ZipfianGenerator {
 public:
  /**
   * The default zipfian constant.
   */
  constexpr static const double kZipfianConst = 0.99;

  /**
   * The maximum number of items.
   */
  constexpr static const uint64_t kMaxNumItems = std::numeric_limits<uint64_t>::max() >> 24U;

  /**
   * Create a generator for items in the range [min, max] inclusive using the given zipfian skew.
   * @param min The smallest integer to generate in the sequence.
   * @param max The largest integer to generate in the sequence.
   * @param zipfian_const The zipfian constant to use.
   */
  ZipfianGenerator(uint64_t min, uint64_t max, double zipfian_const)
      : num_items_(max - min + 1), base_(min), theta_(zipfian_const), zeta_n_(0), n_for_zeta_(0) {
    TPL_ASSERT(num_items_ >= 2 && num_items_ < kMaxNumItems, "Element count out of valid range");
    zeta_2_ = Zeta(2, theta_);
    alpha_ = 1.0 / (1.0 - theta_);
    RaiseZeta(num_items_);
    eta_ = Eta();

    Next();
  }

  /**
   * Create a generator for the specified number of items using the specified skew.
   * @param num_items The number of items in the distribution.
   * @param zipfian_const The zipfian constant to use.
   */
  ZipfianGenerator(uint64_t num_items, double zipfian_const)
      : ZipfianGenerator(0, num_items - 1, zipfian_const) {}

  /**
   * Create a generator for items in the range [min, max] inclusive.
   * @param min The smallest integer to generate in the sequence.
   * @param max The largest integer to generate in the sequence.
   */
  ZipfianGenerator(uint64_t min, uint64_t max) : ZipfianGenerator(min, max, kZipfianConst) {}

  /**
   * Create a generator for the specified number of items.
   * @param num_items The number of items in the distribution.
   */
  explicit ZipfianGenerator(uint64_t num_items) : ZipfianGenerator(0, num_items - 1) {}

  /**
   * @return The next random element.
   */
  uint64_t Next(uint64_t num_items);

  /**
   * @return The next random element.
   */
  uint64_t Next() { return Next(num_items_); }

  /**
   * @return The most recently returned element.
   */
  uint64_t Last();

 private:
  // Compute the zeta constant needed for the distribution.
  // Remember the number of items, so if it is changed, we can recompute zeta.
  void RaiseZeta(uint64_t num) {
    assert(num >= n_for_zeta_);
    zeta_n_ = Zeta(n_for_zeta_, num, theta_, zeta_n_);
    n_for_zeta_ = num;
  }

  double Eta() { return (1 - std::pow(2.0 / num_items_, 1 - theta_)) / (1 - zeta_2_ / zeta_n_); }

  // Calculate the zeta constant needed for a distribution.
  // Do this incrementally from the last_num of items to the cur_num.
  // Use the zipfian constant as theta. Remember the new number of items
  // so that, if it is changed, we can recompute zeta.
  static double Zeta(uint64_t last_num, uint64_t cur_num, double theta, double last_zeta) {
    double zeta = last_zeta;
    for (uint64_t i = last_num + 1; i <= cur_num; ++i) {
      zeta += 1 / std::pow(i, theta);
    }
    return zeta;
  }

  static double Zeta(uint64_t num, double theta) { return Zeta(0, num, theta, 0); }

  static double RandomDouble(double min = 0.0, double max = 1.0) {
    static std::default_random_engine generator(std::random_device{}());
    static std::uniform_real_distribution<double> uniform(min, max);
    return uniform(generator);
  }

 private:
  // Number of elements in range.
  uint64_t num_items_;
  // Min number of items to generate.
  uint64_t base_;

  // Computed parameters for generating the distribution.
  double theta_, zeta_n_, eta_, alpha_, zeta_2_;
  uint64_t n_for_zeta_;  // Number of items used to compute zeta_n.
  uint64_t last_value_;
  std::mutex mutex_;
};

// ---------------------------------------------------------
// Implementation below.
// ---------------------------------------------------------

inline uint64_t ZipfianGenerator::Next(uint64_t num) {
  assert(num >= 2 && num < kMaxNumItems);
  std::lock_guard<std::mutex> lock(mutex_);

  if (num > n_for_zeta_) {  // Recompute zeta_n and eta
    RaiseZeta(num);
    eta_ = Eta();
  }

  double u = RandomDouble();
  double uz = u * zeta_n_;

  if (uz < 1.0) {
    return last_value_ = 0;
  }

  if (uz < 1.0 + std::pow(0.5, theta_)) {
    return last_value_ = 1;
  }

  return last_value_ = base_ + num * std::pow(eta_ * u - eta_ + 1, alpha_);
}

inline uint64_t ZipfianGenerator::Last() {
  std::lock_guard<std::mutex> lock(mutex_);
  return last_value_;
}

}  // namespace tpl::util

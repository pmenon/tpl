#pragma once

#include <chrono>

namespace tpl::util {

template <typename ResolutionRatio = std::ratio<1>>
class Timer {
  using Clock = std::chrono::high_resolution_clock;
  using TimePoint = std::chrono::time_point<Clock>;

 public:
  Timer() noexcept : total_(0), elapsed_(0) { Start(); }

  void Start() noexcept { start_ = Clock::now(); }

  void Stop() noexcept {
    stop_ = Clock::now();

    elapsed_ =
        std::chrono::duration_cast<
            std::chrono::duration<double, ResolutionRatio>>(stop_ - start_)
            .count();

    total_ += elapsed_;
  }

  double elapsed() const noexcept { return elapsed_; }

  double total_elapsed() const noexcept { return total_; }

 private:
  TimePoint start_;
  TimePoint stop_;

  double total_;
  double elapsed_;
};

}  // namespace tpl::util
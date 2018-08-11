#pragma once

#include <chrono>

namespace tpl::util {

template <typename ResolutionRatio = std::ratio<1>>
class StopWatch {
  using Clock = std::chrono::high_resolution_clock;
  using TimePoint = std::chrono::time_point<Clock>;

 public:
  StopWatch() noexcept : elapsed_(0) {}

  void Start() noexcept { start_ = Clock::now(); }

  void Stop() noexcept {
    stop_ = Clock::now();

    elapsed_ =
        std::chrono::duration_cast<
            std::chrono::duration<double, ResolutionRatio>>(stop_ - start_)
            .count();
  }

  double elapsed() const noexcept { return elapsed_; }

 private:
  TimePoint start_;
  TimePoint stop_;

  double elapsed_;
};

}  // namespace tpl::util
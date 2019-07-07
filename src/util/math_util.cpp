#include "util/math_util.h"

#include <cmath>

namespace tpl::util {

bool MathUtil::ApproxEqual(f32 left, f32 right) {
  const float epsilon = std::fabs(right) * 0.01;
  return std::fabs(left - right) <= epsilon;
}

bool MathUtil::ApproxEqual(f64 left, f64 right) {
  double epsilon = std::fabs(right) * 0.01;
  return std::fabs(left - right) <= epsilon;
}

}  // namespace tpl::util

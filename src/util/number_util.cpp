#include "util/number_util.h"

#include <cstring>

#include "common/macros.h"
#include "util/math_util.h"

namespace tpl::util {

namespace {

// A table of all two-digit numbers. Used to speed up decimal digit generation.
// clang-format off
constexpr const char kDigitTable[] =
    "00" "01" "02" "03" "04" "05" "06" "07" "08" "09"
    "10" "11" "12" "13" "14" "15" "16" "17" "18" "19"
    "20" "21" "22" "23" "24" "25" "26" "27" "28" "29"
    "30" "31" "32" "33" "34" "35" "36" "37" "38" "39"
    "40" "41" "42" "43" "44" "45" "46" "47" "48" "49"
    "50" "51" "52" "53" "54" "55" "56" "57" "58" "59"
    "60" "61" "62" "63" "64" "65" "66" "67" "68" "69"
    "70" "71" "72" "73" "74" "75" "76" "77" "78" "79"
    "80" "81" "82" "83" "84" "85" "86" "87" "88" "89"
    "90" "91" "92" "93" "94" "95" "96" "97" "98" "99";
// clang-format on

}  // namespace

// Adapted from http://graphics.stanford.edu/~seander/bithacks.html#IntegerLog10
uint32_t NumberUtil::DecimalLength_32(uint32_t value) {
  static constexpr uint32_t kPowersOfTen[] = {1,      10,      100,      1000,      10000,
                                              100000, 1000000, 10000000, 100000000, 1000000000};

  // Logarithm base change rule: log_10{v} = log_2{v} / log_2{10}
  // Base-two logarithms are simple bit-manipulation.
  // Use a rough approximation of log_2{10}.
  const auto t = (MathUtil::Log2(value) + 1) * 1233 / 4096;
  return t + (value >= kPowersOfTen[t]);
}

uint32_t NumberUtil::NumberToString(char *str, uint32_t value) {
  // Fast path.
  if (value == 0) {
    *str = '0';
    return 1;
  }

  // Total length of string.
  const auto len = DecimalLength_32(value);

  // Index where result is written to.
  uint32_t i = 0;

  // Process 4 digits at a time.
  while (value >= 10000) {
    // Don't be concerned by the modulo and divisions below.
    // GCC/Clang will convert it into shifts and multiplications when
    // the operands are compile-time constants.
    const uint32_t c = value % 10000;
    const uint32_t c_lo = c % 100;
    const uint32_t c_hi = c / 100;
    char *pos = str + len - i;
    std::memcpy(pos - 2, kDigitTable + c_lo * 2, 2);
    std::memcpy(pos - 4, kDigitTable + c_hi * 2, 2);
    value /= 10000;
    i += 4;
  }

  if (value >= 100) {
    const uint32_t c = value % 100;
    char *pos = str + len - i;
    std::memcpy(pos - 2, kDigitTable + c * 2, 2);
    value /= 100;
    i += 2;
  }

  if (value >= 10) {
    char *pos = str + len - i;
    std::memcpy(pos - 2, kDigitTable + value * 2, 2);
  } else {
    *str = (char)('0' + value);
  }

  return len;
}

char *NumberUtil::NumberToStringWithZeroPad(char *str, uint32_t value, uint32_t min_width) {
  TPL_ASSERT(min_width > 0, "Minimum width must be greater than zero.");
  // Fast path for two-digit numbers.
  if (value < 100 && min_width == 2) {
    std::memcpy(str, kDigitTable + value * 2, 2);
    return str + 2;
  }

  // Write string. If the length is larger than the minimum width, we're done.
  const auto len = NumberToString(str, value);
  if (len >= min_width) {
    return str + len;
  }

  // The length is shorter than the requested width. Need to pad.
  std::memmove(str + min_width - len, str, len);
  std::memset(str, '0', min_width - len);
  return str + min_width;
}

}  // namespace tpl::util
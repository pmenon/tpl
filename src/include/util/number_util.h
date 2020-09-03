#pragma once

#include "common/common.h"

namespace tpl::util {

class NumberUtil : public AllStatic {
 public:
  /** Maximum length of an 8-byte (64-bit) number, including sign. */
  static constexpr std::size_t kMaxInt8Len = 20;

  /** Maximum length of a 4-byte (32-bit) number, including sign. */
  static constexpr std::size_t kMaxInt4Len = 11;
  
  /**
   * @return The length of the string required to store the given 32-bit value @em value.
   */
  static uint32_t DecimalLength_32(uint32_t value);

  /**
   * Converts @em value into a decimal string representation written to @em str and returns the
   * length of the result.
   * @pre Input buffer must be sufficiently large to store the result, i.e., 10 bytes for 32-bit.
   * @param str Where the result is written to.
   * @param value The integer value to write.
   * @return The length of the resulting string.
   */
  static uint32_t NumberToString(char *str, uint32_t value);

  /**
   * Converts @em value into a decimal string representation written to @em str. If the result is
   * less than @em min_width, any extra space is filled up by prefixing the number with zeros.
   * @param str Where the result is written to.
   * @param value The integer value to write.
   * @param min_width The minimum width.
   * @return The ending address of the string result (i.e., the last character written plus 1). No
   *         null-terminating character is written.
   */
  static char *NumberToStringWithZeroPad(char *str, uint32_t value, uint32_t min_width);
};

}  // namespace tpl::util
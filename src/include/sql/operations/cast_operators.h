#pragma once

#include <string>

#include "common/common.h"
#include "common/exception.h"
#include "sql/runtime_types.h"

namespace tpl::sql {

template <bool IntOverflow = false,    // Default: No integer overflow casts.
          bool FloatTruncate = false,  // Default: No floating point truncation.
          bool TimeTruncate = false,   // Default: No time truncation.
          bool TimeOverflow = false,   // Default: No time overflow.
          bool InvalidUtf8 = false>    // Default: No invalid UTF-8 characters.
struct CastOptions {
  static constexpr bool AllowIntOverflow() noexcept { return IntOverflow; }
  static constexpr bool AllowFloatTruncate() noexcept { return FloatTruncate; }
};

//===----------------------------------------------------------------------===//
//
// Checked Cast.
//
//===----------------------------------------------------------------------===//

/**
 * TryCast is a function object implementing a "safe" cast. On invocation, it will attempt to cast
 * its single input with type @em InType into a value of type @em OutType. If the cast operation is
 * valid, it returns true and the output result parameter is set to the casted value. If the cast
 * produces a value NOT in the valid range for the output type, the cast return false. The output
 * parameter is in an invalid state.
 * @tparam InType The CPP type of the input.
 * @tparam OutType The CPP type the input type into.
 * @tparam Options Configurable casting options.
 */
template <typename InType, typename OutType, typename Options = CastOptions<>,
          typename Enable = void>
struct TryCast {};

/**
 * Cast is a function object implementing a "checking" cast. On invocation, it attempts to cast its
 * input with type @em InType into a value of type @em OutType. If valid, the resulting value is
 * returned. If the cast is invalid, a ValueOutOfRangeException exception is thrown,
 * @tparam InType The CPP type of the input.
 * @tparam OutType The CPP type the input type into.
 * @tparam Options Configurable casting options.
 */
template <typename InType, typename OutType, typename Options = CastOptions<>>
struct Cast {
  OutType operator()(InType input) const {
    OutType result;
    if (!TryCast<InType, OutType, Options>{}(input, &result)) {
      throw ValueOutOfRangeException(GetTypeId<InType>(), GetTypeId<OutType>());
    }
    return result;
  }
};

//===----------------------------------------------------------------------===//
//
// Trivial Cast
//
//===----------------------------------------------------------------------===//

template <typename T, typename Options>
struct TryCast<T, T, Options> {
  bool operator()(const T input, T *output) const noexcept {
    *output = input;
    return true;
  }
};

namespace detail {

// Is the given type an integer type? Note: we cannot use std::is_integral<>
// because it includes the primitive bool type. We want to distinguish only
// primitive integer types.
template <typename T>
struct is_integer_type
    : std::integral_constant<bool, std::is_integral_v<T> && !std::is_same_v<bool, T>> {};

template <typename T>
constexpr bool is_integer_type_v = is_integer_type<T>::value;

// Is the given template type a floating point type?
template <typename T>
struct is_floating_type : std::integral_constant<bool, std::is_floating_point_v<T>> {};

template <typename T>
constexpr bool is_floating_type_v = is_floating_type<T>::value;

// Is the given type either an integer type or a floating point type?
template <typename T>
struct is_number_type
    : std::integral_constant<bool, is_integer_type_v<T> || std::is_floating_point_v<T>> {};

template <typename T>
constexpr bool is_number_type_v = is_number_type<T>::value;

// Is the cast from the given input and output types a downward cast?
template <typename InType, typename OutType>
struct is_number_downcast {
  static constexpr bool value =
      // Both types are numbers.
      is_number_type_v<InType> && is_number_type_v<OutType> &&
      // Both have the same signed-ness.
      std::is_signed_v<InType> == std::is_signed_v<OutType> &&
      // Both have the same integer-ness.
      std::is_floating_point_v<InType> == std::is_floating_point_v<OutType> &&
      // The output type has a smaller domain the input. We use storage size to determine this.
      sizeof(OutType) < sizeof(InType);
};

// Is the cast from the given input type to the output type a cast from a signed
// to an unsigned integer type?
template <typename InType, typename OutType>
struct is_integral_signed_to_unsigned {
  static constexpr bool value =
      // Both types are integers (non-bool and non-float).
      is_integer_type_v<InType> && is_integer_type_v<OutType> &&
      // The input is signed and output is unsigned.
      std::is_signed_v<InType> && std::is_unsigned_v<OutType>;
};

template <typename InType, typename OutType>
struct is_integral_unsigned_to_signed {
  static constexpr bool value = is_integer_type_v<InType> && is_integer_type_v<OutType> &&
                                std::is_unsigned_v<InType> && std::is_signed_v<OutType>;
};

template <typename InType, typename OutType>
struct is_safe_numeric_cast {
  static constexpr bool value =
      // Both inputs are numbers.
      is_number_type_v<InType> && is_number_type_v<OutType> &&
      // Both have the same signed-ness.
      std::is_signed_v<InType> == std::is_signed_v<OutType> &&
      // Both have the same integer-ness.
      std::is_integral_v<InType> == std::is_integral_v<OutType> &&
      // The output type has a larger domain then input. We use storage size to determine this.
      sizeof(OutType) >= sizeof(InType) &&
      // They're different types.
      !std::is_same_v<InType, OutType>;
};

template <typename InType, typename OutType>
struct is_float_truncate {
  static constexpr bool value =
      // The input is an integer and the output is a float.
      (is_integer_type_v<InType> && is_floating_type_v<OutType>) ||
      // Or, the input is float and output is an integer.
      (is_floating_type_v<InType> && is_integer_type_v<OutType>);
};

}  // namespace detail

//===----------------------------------------------------------------------===//
//
// Numeric -> Boolean
//
//===----------------------------------------------------------------------===//

/**
 * Cast a numeric value into a boolean.
 * @tparam InType The numeric input type.
 * @tparam Options Trait capturing options for how the cast should be performed.
 */
template <typename InType, typename Options>
struct TryCast<
    InType, bool, Options,
    std::enable_if_t<detail::is_number_type_v<InType> && !std::is_same_v<InType, bool>>> {
  bool operator()(const InType input, bool *output) noexcept {
    *output = static_cast<bool>(input);
    return true;
  }
};

//===----------------------------------------------------------------------===//
//
// Boolean -> Numeric
//
//===----------------------------------------------------------------------===//

/**
 * Cast a boolean into a numeric value.
 * @tparam OutType The numeric output type.
 * @tparam Options Trait capturing options for how the cast should be performed.
 */
template <typename OutType, typename Options>
struct TryCast<bool, OutType, Options, std::enable_if_t<detail::is_number_type_v<OutType>>> {
  bool operator()(const bool input, OutType *output) const noexcept {
    *output = static_cast<OutType>(input);
    return true;
  }
};

//===----------------------------------------------------------------------===//
//
// Numeric cast.
//
// These casts are grouped into four categories:
// 1. Down-cast.
// 2. Signed-to-unsigned cast.
// 3. Unsigned-to-signed cast.
// 4. Floating point truncation.
//
//===----------------------------------------------------------------------===//

/**
 * Numeric down-cast.
 * @tparam InType The numeric input type. Must satisfy internal::is_number_type_v<InType>.
 * @tparam OutType The numeric output type.  Must satisfy internal::is_number_type_v<OutType>.
 * @tparam Options Casting options.
 */
template <typename InType, typename OutType, typename Options>
struct TryCast<InType, OutType, Options,
               std::enable_if_t<detail::is_number_downcast<InType, OutType>::value ||
                                detail::is_integral_signed_to_unsigned<InType, OutType>::value ||
                                detail::is_integral_unsigned_to_signed<InType, OutType>::value>> {
  bool operator()(const InType input, OutType *output) const noexcept {
    constexpr OutType kMin = std::numeric_limits<OutType>::lowest();
    constexpr OutType kMax = std::numeric_limits<OutType>::max();

    *output = static_cast<OutType>(input);
    if constexpr (Options::AllowIntOverflow()) {
      return true;
    } else {
      return input >= kMin && input <= kMax;
    }
  }
};

/**
 * Safe numeric up-cast, i.e., a regular cast.
 * @tparam InType The numeric input type. Must satisfy internal::is_number_type_v<InType>.
 * @tparam OutType The numeric output type.  Must satisfy internal::is_number_type_v<OutType>.
 * @tparam Options Casting options.
 */
template <typename InType, typename OutType, typename Options>
struct TryCast<InType, OutType, Options,
               std::enable_if_t<detail::is_safe_numeric_cast<InType, OutType>::value &&
                                !detail::is_number_downcast<InType, OutType>::value>> {
  bool operator()(const InType input, OutType *output) const noexcept {
    *output = static_cast<OutType>(input);
    return true;
  }
};

/**
 * Floating-point to integer (or vice-versa) cast.
 * @tparam InType The numeric input type. Must satisfy internal::is_number_type_v<InType>.
 * @tparam OutType The numeric output type.  Must satisfy internal::is_number_type_v<OutType>.
 * @tparam Options Casting options.
 */
template <typename InType, typename OutType, typename Options>
struct TryCast<InType, OutType, Options,
               std::enable_if_t<detail::is_float_truncate<InType, OutType>::value>> {
  bool operator()(const InType input, OutType *output) const noexcept {
    *output = static_cast<OutType>(input);
    if constexpr (Options::AllowFloatTruncate()) {
      return true;
    } else {
      return static_cast<InType>(*output) == input;
    }
  }
};

/**
 * Numeric value to Date. This isn't a real cast, but let's support it for now.
 * @tparam InType The numeric input type. Must satisfy internal::is_number_type_v<InType>
 * @tparam Options Casting options.
 */
template <typename InType, typename Options>
struct TryCast<InType, Date, Options, std::enable_if_t<detail::is_integer_type_v<InType>>> {
  bool operator()(const InType input, Date *output) const noexcept {
    *output = Date(input);
    return true;
  }
};

/**
 * Boolean or numeric value to string.
 * @tparam InType The input type. Either a number or a boolean.
 * @tparam Options Casting options.
 */
template <typename InType, typename Options>
struct TryCast<InType, std::string, Options,
               std::enable_if_t<detail::is_number_type_v<InType> || std::is_same_v<InType, bool>>> {
  bool operator()(const InType input, std::string *output) const noexcept {
    *output = std::to_string(input);
    return true;
  }
};

//===----------------------------------------------------------------------===//
//
// Date or Timestamp to string
//
//===----------------------------------------------------------------------===//

/**
 * Date or Timestamp to string conversion.
 * @tparam Options Casting options.
 */
template <typename InType, typename Options>
struct TryCast<
    InType, std::string, Options,
    std::enable_if_t<std::is_same_v<InType, Date> || std::is_same_v<InType, Timestamp>>> {
  bool operator()(const InType input, std::string *output) const noexcept {
    *output = input.ToString();
    return true;
  }
};

/**
 * Date to Timestamp conversion.
 * @tparam Options Casting options.
 */
template <typename Options>
struct TryCast<Date, Timestamp, Options> {
  bool operator()(const Date input, Timestamp *output) const noexcept {
    *output = input.ConvertToTimestamp();
    return true;
  }
};

/**
 * Timestamp to Date conversion.
 * @tparam Options Casting options.
 */
template <typename Options>
struct TryCast<Timestamp, Date, Options> {
  bool operator()(const Timestamp input, Date *output) const noexcept {
    *output = input.ConvertToDate();
    return true;
  }
};

}  // namespace tpl::sql

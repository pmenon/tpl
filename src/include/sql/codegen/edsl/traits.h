#pragma once

#include "sql/codegen/edsl/fwd.h"

namespace tpl::sql::codegen::edsl {

namespace trait_details {

template <typename T>
struct IsInteger : std::false_type {};

// clang-format off
template <> struct IsInteger<Int8> : std::true_type {};
template <> struct IsInteger<Int16> : std::true_type {};
template <> struct IsInteger<Int32> : std::true_type {};
template <> struct IsInteger<Int64> : std::true_type {};
template <> struct IsInteger<UInt8> : std::true_type {};
template <> struct IsInteger<UInt16> : std::true_type {};
template <> struct IsInteger<UInt32> : std::true_type {};
template <> struct IsInteger<UInt64> : std::true_type {};
// clang-format on

template <typename T>
struct IsFloatingPoint : std::false_type {};

// clang-format off
template <> struct IsFloatingPoint<Float32> : std::true_type {};
template <> struct IsFloatingPoint<Float64> : std::true_type {};
// clang-format on

#if 0
template <typename T>
struct IsSQLValue : std::false_type {};

// clang-format off
template <> struct IsSQLValue<edsl::Bool> : std::true_type {};
template <> struct IsSQLValue<edsl::Integer> : std::true_type {};
template <> struct IsSQLValue<edsl::Real> : std::true_type {};
template <> struct IsSQLValue<edsl::Decimal> : std::true_type {};
template <> struct IsSQLValue<edsl::Date> : std::true_type {};
template <> struct IsSQLValue<edsl::Timestamp> : std::true_type {};
template <> struct IsSQLValue<edsl::String> : std::true_type {};
// clang-format on
#endif

}  // namespace trait_details

/**
 * Trait to get information about EDSL expression types.
 *
 * For non-ELT types, is_etl is false and in that case, no other fields should be used on the
 * traits.
 *
 * @tparam T the type to introspect.
 */
template <typename T, typename Enable = void>
struct Traits {
  using ValueType = T;
  /** Indicates if T is an ETL type. */
  static constexpr bool kIsETL = false;
};

template <typename T>
using DecayTraits = Traits<std::decay_t<T>>;

/**
 * Traits to extract the value type out of an ETL type.
 */
template <typename T>
using ValueT = typename DecayTraits<T>::ValueType;

template <typename T>
constexpr bool IsETLValueClass = std::is_base_of_v<Value, T>;
/**
 * Trait indicating if the given type is an ETL expression.
 * @tparam T The type to test.
 */
template <typename T>
constexpr bool IsETLExpr = DecayTraits<T>::kIsETL;

/**
 * Trait indicating all the provided types are ETL expression.
 * @tparam T The types to test.
 */
template <typename... Ts>
constexpr bool AllETLExpr = (IsETLExpr<Ts> && ...);

/**
 * Trait indicating if the given input types have the same expression value types.
 * @tparam T The left input.
 * @tparam U The right input.
 */
template <typename T, typename U>
constexpr bool SameValueType = std::is_same_v<ValueT<T>, ValueT<U>>;

};  // namespace tpl::sql::codegen::edsl

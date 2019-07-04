#pragma once

namespace tpl::sql {

struct BitwiseXor {
  template <typename T>
  static T Apply(T a, T b) {
    return a ^ b;
  }
};

struct BitwiseAnd {
  template <typename T>
  static T Apply(T a, T b) {
    return a & b;
  }
};

struct BitwiseOr {
  template <typename T>
  static T Apply(T a, T b) {
    return a | b;
  }
};

struct BitwiseShiftLeft {
  template <typename T>
  static T Apply(T a, T b) {
    return a << b;
  }
};

struct BitwiseShiftRight {
  template <typename T>
  static T Apply(T a, T b) {
    return a >> b;
  }
};

struct BitwiseNot {
  template <typename T>
  static T Apply(T input) {
    return ~input;
  }
};

}  // namespace tpl::sql
#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <variant>

#include "common/common.h"
#include "common/macros.h"

namespace tpl {

// The list of all settings in the engine. The two arguments to the macro are both callback
// functions, but with different signatures.
//
// CONST     : Callback function assuming the setting is a constant hard-coded value.
//             Args: setting name, C/C++ primitive type, default value
// COMPUTED  : Callback function assuming default value is computed at runtime.
//             Args: setting name, C/C++ primitive type, provider function.
#define SETTINGS_LIST(CONST, COMPUTED)                                                             \
  /*                                                                                               \
   * When performing selections, rather than only operating on active elements from a selection    \
   * vector or TID lists, it may be faster to apply the comparison on all elements and intersect   \
   * the result with the input list because. Such an optimization removes branches from the inner- \
   * loop and enables the compiled to auto-vectorize the selection. However, this optimization     \
   * wins only when the selectivity of the input selection vector or TID list is sufficiently      \
   * large.                                                                                        \
   */                                                                                              \
  COMPUTED(SelectOptThreshold, double, DeriveOptimalFullSelectionThreshold)                        \
                                                                                                   \
  /*                                                                                               \
   * Selections can use either a branching or branch-less strategy. Experimentally, branch-less    \
   * selections work best when the selectivity of the predicate is in the range [0.1, 0.9].        \
   */                                                                                              \
  CONST(SelectBranchingThreshold_Low, double, 0.1)                                                 \
  CONST(SelectBranchingThreshold_High, double, 0.9)

class Settings {
 public:
  // List of all settings
  enum class Name : uint32_t {
#define F(NAME, ...) NAME,
    SETTINGS_LIST(F, F)
#undef F
  };

  // Setting values are stored as glorified unions
  using Value = std::variant<bool, int64_t, double, std::string>;

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(Settings);

  /**
   * Singleton instance access.
   * @return The only instance of the settings object.
   */
  static Settings *Instance() {
    static Settings kInstance;
    return &kInstance;
  }

  /**
   * Retrieve the value of the setting with name @em name. If no setting with the given name exists,
   * or the setting is not stored as a boolean, an empty/missing value is returned.
   * @param name The name of the value to read.
   * @return The value of the setting, if it exists.
   */
  std::optional<bool> GetBool(Name name) const;

  /**
   * Retrieve the value of the setting with name @em name. If no setting with the given name exists,
   * or the setting is not stored as an integer, an empty/missing value is returned.
   * @param name The name of the value to read.
   * @return The value of the setting, if it exists.
   */
  std::optional<int64_t> GetInt(Name name) const;

  /**
   * Retrieve the value of the setting with name @em name. If no setting with the given name exists,
   * or the setting is not stored as a floating-point number, an empty/missing value is returned.
   * @param name The name of the value to read.
   * @return The value of the setting, if it exists.
   */
  std::optional<double> GetDouble(Name name) const;

  /**
   * Retrieve the value of the setting with name @em name. If no setting with the given name exists,
   * or the setting is not stored as a string, an empty/missing value is returned.
   * @param name The name of the value to read.
   * @return The value of the setting, if it exists.
   */
  std::optional<std::string> GetString(Name name) const;

 private:
  // Private to force singleton access
  Settings();

  template <typename T>
  std::optional<T> GetSettingValueImpl(Name name) const;

 private:
  // Container for all settings
  std::unordered_map<Name, Value> settings_;
};

}  // namespace tpl

#include "common/settings.h"

#include <string>

#include "common/cpu_info.h"

namespace tpl {

namespace {

double DeriveOptimalFullSelectionThreshold(UNUSED Settings *settings, UNUSED CpuInfo *cpu_info) {
  return 0.25;
}

}  // namespace

Settings::Settings() {
  // First the constant setting values
#define CONST_SETTING(NAME, TYPE, VALUE) settings_[Settings::Name::NAME] = VALUE;
#define COMPUTED_SETTING(...)
  SETTINGS_LIST(CONST_SETTING, COMPUTED_SETTING)
#undef CONST_SETTING
#undef COMPUTED_SETTING

  // Now the computed settings
#define CONST_SETTING(...)
#define COMPUTED_SETTING(NAME, TYPE, GEN_FN) \
  settings_[Settings::Name::NAME] = GEN_FN(this, CpuInfo::Instance());
  SETTINGS_LIST(CONST_SETTING, COMPUTED_SETTING)
#undef CONST_SETTING
#undef COMPUTED_SETTING
}

template <typename T>
std::optional<T> Settings::GetSettingValueImpl(Settings::Name name) const {
  const auto iter = settings_.find(name);
  return iter == settings_.end() ? std::optional<T>{} : std::get<T>(iter->second);
}

std::optional<bool> Settings::GetBool(Settings::Name name) const {
  return GetSettingValueImpl<bool>(name);
}

std::optional<int64_t> Settings::GetInt(Settings::Name name) const {
  return GetSettingValueImpl<int64_t>(name);
}

std::optional<double> Settings::GetDouble(Settings::Name name) const {
  return GetSettingValueImpl<double>(name);
}

std::optional<std::string> Settings::GetString(Settings::Name name) const {
  return GetSettingValueImpl<std::string>(name);
}

}  // namespace tpl

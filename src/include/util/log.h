#pragma once

#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

namespace tpl {

class Logger {
 public:
  Logger(const std::string &name) : logger_(spdlog::stdout_color_mt(name)) {}

  template <typename... Args>
  void debug(const char *msg, const Args... args) {
    logger_->debug(msg, std::forward(args)...);
  }

  template <typename... Args>
  void info(const char *msg, const Args... args) {
    logger_->info(msg, std::forward(args)...);
  }

  template <typename... Args>
  void warn(const char *msg, const Args... args) {
    logger_->warn(msg, std::forward(args)...);
  }

  template <typename... Args>
  void error(const char *msg, const Args... args) {
    logger_->error(msg, std::forward(args)...);
  }

  template <typename... Args>
  void critical(const char *msg, const Args... args) {
    logger_->critical(msg, std::forward(args)...);
  }

 private:
  std::shared_ptr<spdlog::logger> logger_;
};
}  // namespace tpl
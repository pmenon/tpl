#include "logging/logger.h"

#ifdef _WIN32
#include <spdlog/sinks/wincolor_sink.h>
#else
#include <spdlog/sinks/ansicolor_sink.h>
#endif

#include <memory>

namespace tpl::logging {

std::shared_ptr<spdlog::logger> logger;

void InitLogger() {
#ifdef _WIN32
  auto color_sink = std::make_shared<sinks::wincolor_stdout_sink_mt>();
#else
  auto color_sink = std::make_shared<spdlog::sinks::ansicolor_stdout_sink_mt>();
#endif

  // The top level logger.
  logger = std::make_shared<spdlog::logger>("logger", std::move(color_sink));
  logger->set_pattern("[%Y-%m-%d %T.%e] [%^%l%$] %v");
  // logger->set_level(SPD_LOG_LEVEL);

  spdlog::register_logger(logger);
}

void ShutdownLogger() { spdlog::shutdown(); }

}  // namespace tpl::logging

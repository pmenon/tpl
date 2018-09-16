#include "logging/logger.h"

namespace tpl::logging {

std::shared_ptr<spdlog::sinks::stdout_sink_mt> default_sink;
std::shared_ptr<spdlog::logger> logger;

void init_logger() {
  // create the default, shared sink
  default_sink = std::make_shared<spdlog::sinks::stdout_sink_mt>();

  // the terrier, top level logger
  logger = std::make_shared<spdlog::logger>("logger", default_sink);
  spdlog::register_logger(logger);
}

}  // namespace tpl::logging

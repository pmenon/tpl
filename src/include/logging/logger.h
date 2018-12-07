#pragma once

#include <memory>

#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

namespace tpl::logging {

// flush the debug logs, every <n> seconds
#define DEBUG_LOG_FLUSH_INTERVAL 3

extern const char *kLoggerName;
extern std::shared_ptr<spdlog::sinks::stdout_sink_mt> default_sink;
extern std::shared_ptr<spdlog::logger> logger;

void InitLogger();

#define LOG_TRACE(...) ::tpl::logging::logger->trace(__VA_ARGS__);

#define LOG_DEBUG(...) ::tpl::logging::logger->debug(__VA_ARGS__);

#define LOG_INFO(...) ::tpl::logging::logger->info(__VA_ARGS__);

#define LOG_WARN(...) ::tpl::logging::logger->warn(__VA_ARGS__);

#define LOG_ERROR(...) ::tpl::logging::logger->error(__VA_ARGS__);

}  // namespace tpl::logging
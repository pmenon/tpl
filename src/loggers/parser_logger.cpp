#include "loggers/parser_logger.h"
#include <memory>
#include "loggers/main_logger.h"

namespace tpl::parser {

std::shared_ptr<spdlog::logger> parser_logger;

void init_parser_logger() {
  parser_logger =
      std::make_shared<spdlog::logger>("parser_logger", ::default_sink);
  spdlog::register_logger(parser_logger);
}

}  // namespace tpl::parser

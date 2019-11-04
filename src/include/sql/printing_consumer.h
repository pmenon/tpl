#pragma once

#include <functional>
#include <iosfwd>
#include <vector>

#include "sql/result_consumer.h"

namespace tpl::sql {

class Schema;

/**
 * Consumer that prints out results to the given output stream.
 */
class PrintingConsumer : public ResultConsumer {
 public:
  /**
   * Create a new consumer.
   * @param os The stream to write the results into.
   * @param output_schema The schema of the output of the query.
   */
  PrintingConsumer(std::ostream &os, const sql::Schema &output_schema);

  /**
   * Print out the tuples in the input batch.
   * @param batch The batch of result tuples to print.
   */
  void Consume(const OutputBuffer &batch) override;

 private:
  using PrintFunc = const byte *(*)(std::ostream &, const byte *);

  // The output stream
  std::ostream &os_;

  // The output schema
  const sql::Schema &output_schema_;

  // Per-column printing functions
  std::vector<PrintFunc> col_printers_;
};

}  // namespace tpl::sql

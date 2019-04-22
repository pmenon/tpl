#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "parser/sql_statement.h"

// TODO(WAN): this file is messy
namespace tpl {
namespace parser {

/**
 * Function parameter.
 */
struct Parameter {
  // TODO(WAN): there used to be a FuncParamMode that was never used?

  /**
   * Parameter data types.
   */
  enum class DataType {
    INT,
    INTEGER,
    TINYINT,
    SMALLINT,
    BIGINT,
    CHAR,
    DOUBLE,
    FLOAT,
    DECIMAL,
    VARCHAR,
    TEXT,
    BOOL,
    BOOLEAN
  };

  /**
   * @param datatype data type of the parameter
   */
  explicit Parameter(DataType datatype) : datatype_(datatype) {}

  virtual ~Parameter() = default;

  /**
   * @return data type of the parameter
   */
  DataType GetDataType() { return datatype_; }

 private:
  const DataType datatype_;
};

/**
 * Function return type.
 */
struct ReturnType : Parameter {
  /**
   * @param datatype data type of the parameter
   */
  explicit ReturnType(DataType datatype) : Parameter(datatype) {}
  ~ReturnType() override = default;
};

/**
 * Function parameter.
 */
struct FuncParameter : Parameter {
  /**
   * @param datatype data type of the parameter
   * @param name name of the function parameter
   */
  FuncParameter(DataType datatype, std::string name)
      : Parameter(datatype), name_(std::move(name)) {}
  ~FuncParameter() override = default;

  /**
   * @return function parameter name
   */
  std::string GetParamName() { return name_; }

 private:
  const std::string name_;
};

/**
 * Represents the sql "CREATE FUNCTION ...".
 */
class CreateFunctionStatement : public SQLStatement {
 public:
  /**
   * @param replace true if it should be replacing the old definition
   * @param func_name function name
   * @param func_body function body
   * @param return_type function return type
   * @param func_parameters function parameters
   * @param pl_type UDF language type
   * @param as_type executable or query string
   */
  CreateFunctionStatement(
      bool replace, std::string func_name, std::vector<std::string> func_body,
      std::shared_ptr<ReturnType> return_type,
      std::vector<std::shared_ptr<FuncParameter>> func_parameters,
      PLType pl_type, AsType as_type)
      : SQLStatement(StatementType::CREATE_FUNC),
        replace_(replace),
        func_name_(std::move(func_name)),
        return_type_(std::move(return_type)),
        func_body_(std::move(func_body)),
        func_parameters_(std::move(func_parameters)),
        pl_type_(pl_type),
        as_type_(as_type) {}

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return true if this function should replace existing definitions
   */
  bool ShouldReplace() { return replace_; }

  /**
   * @return function name
   */
  std::string GetFuncName() { return func_name_; }

  /**
   * @return return type
   */
  std::shared_ptr<ReturnType> GetFuncReturnType() { return return_type_; }

  /**
   * @return function body
   */
  std::vector<std::string> GetFuncBody() { return func_body_; }

  /**
   * @return function parameters
   */
  std::vector<std::shared_ptr<FuncParameter>> GetFuncParameters() {
    return func_parameters_;
  }

  /**
   * @return programming language type
   */
  PLType GetPLType() { return pl_type_; }

  /**
   * @return as type (executable or query string)
   */
  AsType GetAsType() { return as_type_; }

 private:
  const bool replace_ = false;
  const std::string func_name_;
  const std::shared_ptr<ReturnType> return_type_;
  const std::vector<std::string> func_body_;
  const std::vector<std::shared_ptr<FuncParameter>> func_parameters_;
  const PLType pl_type_;
  const AsType as_type_;
};

}  // namespace parser
}  // namespace tpl

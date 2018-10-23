#pragma once

#include <cstdint>

#include "ast/identifier.h"
#include "util/common.h"

namespace tpl {

namespace ast {
class Type;
}  // namespace ast

namespace sema {
/*
 * The following macro lists out all the semantic and syntactic error messages
 * in TPL. Each macro has three parts: a globally unique textual message ID, the
 * templated string message that will be displayed, and the types of each
 * template argument.
 */
#define MESSAGE_LIST(F)                                                        \
  F(UnexpectedToken, "unexpected token '%0', expecting '%1'",                  \
    (parsing::Token::Type, parsing::Token::Type))                              \
  F(DuplicateArgName, "duplicate named argument '%0' in function '%0'",        \
    (ast::Identifier))                                                         \
  F(DuplicateStructFieldName, "duplicate field name '%0' in struct '%1'",      \
    (ast::Identifier, ast::Identifier))                                        \
  F(AssignmentUsedAsValue, "assignment '%0' = '%1' used as value",             \
    (ast::Identifier, ast::Identifier))                                        \
  F(ExpectingExpression, "expecting expression", ())                           \
  F(ExpectingType, "expecting type", ())                                       \
  F(InvalidOperation, "invalid operation: '%0' on type '%1'",                  \
    (parsing::Token::Type, ast::Type *))                                       \
  F(VariableRedeclared, "'%0' redeclared in this block", (ast::Identifier))    \
  F(UndefinedVariable, "undefined: '%0'", (ast::Identifier))                   \
  F(NonFunction, "cannot call non-function '%0'", ())                          \
  F(NotEnoughCallArgs, "not enough arguments in call to '%0'",                 \
    (ast::Identifier))                                                         \
  F(TooManyCallArgs, "too many arguments in call to '%0'", (ast::Identifier))  \
  F(IncorrectCallArgType,                                                      \
    "cannot use a '%0' as type '%1' in argument to '%2'",                      \
    (ast::Type *, ast::Type *, ast::Identifier))                               \
  F(NonBoolIfCondition, "non-bool used as if condition", ())                   \
  F(NonBoolForCondition, "non-bool used as for condition", ())                 \
  F(NonIntegerArrayLength, "non-integer literal used as array size", ())       \
  F(NegativeArrayLength, "array bound must be non-negative", ())               \
  F(ReturnOutsideFunction, "return outside function", ())                      \
  F(MissingTypeAndInitialValue,                                                \
    "variable '%0' must have either a declared type or an initial value",      \
    (ast::Identifier))                                                         \
  F(MismatchedTypesToBinary,                                                   \
    "mismatched types '%0' and '%1' to binary operation '%2'",                 \
    (ast::Type *, ast::Type *, parsing::Token::Type))                          \
  F(MismatchedReturnType,                                                      \
    "type of 'return' expression '%0' incomptible with function's declared "   \
    "return type '%1'",                                                        \
    (ast::Type *, ast::Type *))                                                \
  F(NonIdentifierIterator, "expected identifier for table iteration variable", \
    ())                                                                        \
  F(NonIdentifierTargetInForInLoop,                                            \
    "target of for-in loop must be an identifier", ())                         \
  F(NonExistingTable, "table with name '%0' does not exist",                   \
    (ast::Identifier))                                                         \
  F(ExpectedIdentifierForSelector, "expected identifier for selector", ())     \
  F(SelObjectNotComposite,                                                     \
    "object of selector has type ('%0') which is not a composite",             \
    (ast::Type *))                                                             \
  F(FieldObjectDoesNotExist,                                                   \
    "No field with name '%0' exists in composite type '%1'",                   \
    (ast::Identifier, ast::Type *))

/**
 * Define the ErrorMessageId enumeration
 */
#define F(id, str, arg_types) id,
enum class ErrorMessageId : u16 { MESSAGE_LIST(F) };
#undef F

/**
 * A templated struct that captures the ID of an error message and the
 * argument types that must be supplied when the error is reported. The
 * template arguments allow us to type-check to make sure users are providing
 * all info.
 */
template <typename... ArgTypes>
struct ErrorMessage {
  const ErrorMessageId id;
};

/**
 * A container for all TPL error messages
 */
class ErrorMessages {
  template <typename T>
  struct ReflectErrorMessageWithDetails;

  template <typename... ArgTypes>
  struct ReflectErrorMessageWithDetails<void(ArgTypes...)> {
    using type = ErrorMessage<ArgTypes...>;
  };

 public:
#define MSG(kind, str, arg_types)                                              \
  static constexpr const ReflectErrorMessageWithDetails<void(arg_types)>::type \
      k##kind = {ErrorMessageId::kind};
  MESSAGE_LIST(MSG)
#undef MSG
};

}  // namespace sema
}  // namespace tpl
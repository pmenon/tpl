#pragma once

#include <string_view>

#include "sql/codegen/edsl/ops.h"
#include "sql/codegen/edsl/value.h"
#include "sql/codegen/edsl/value_vt.h"

namespace tpl::sql::codegen::edsl {

/**
 * <h2>Overview:</h2>
 * This class represents a struct data type in TPL code and is the primary (and only) interface to
 * using runtime-defined structures in the TPL EDSL.
 *
 * <h2>Definition Phase:</h2>
 * This class is typically used in two phases: a definition phase and a use phase. In the definition
 * phase, users adds members to the struct through AddMember() to fill out its definition.
 *
 * For example:
 *
 * @code
 * // The constructor begins the definition of a struct called "MyStruct".
 * Struct my_struct(codegen, "MyStruct", true);
 * my_struct.AddMember("a", codegen->GetType<bool>());                 // a: bool
 * my_struct.AddMember("b", codegen->GetType<int64_t>());              // b: int64_t
 * my_struct.AddMember("c", codegen->GetType<float>());                // c: float32
 * my_struct.AddMember("d", codegen->GetType<ast::x::StringVal>());    // d: StringVal
 * my_struct.Seal();
 * @endcode
 *
 * The above example defines a TPL struct with four members. Members can be any valid TPL type,
 * including primitive C++ types, or TPL SQL types (e.g., "d" as a sql::StringVal), or even complex
 * types like AggregationHashTables.
 *
 * <h3>Names and Types:</h3>
 * Although callers may provide names of the members, the implementation does not guarantee that the
 * name of the member in the final constructed type will match that which was provided at the time
 * AddMember() was called. For this reason, users must use the MemberId that was returned when the
 * member was initially added.
 *
 * Any valid TPL type can be added as a member of a TPL struct. When using non-primitive types, the
 * proxy system must be used. For example, to add a tpl::sql::JoinHashTable member, the type must be
 * a tpl::ast::x::JoinHashTable type.
 *
 * <h3>Optimization:</h3>
 * If the "optimize_layout" flag is true during instantiation, this struct implementation may
 * potentially re-arrange the layout of the structure to optimize for CPU or memory performance.
 *
 * <h2>Usage Phase:</h2>
 */
class Struct {
 public:
  /**
   * Typedef used to reference members of the struct.
   */
  using MemberId = uint32_t;

  /**
   * Create a new struct type with the given name.
   * @param codegen The code generator instance.
   * @param name The name of the struct.
   * @param optimize_layout A boolean flag indicating whether the struct is allowed to optimize its
   *                        layout for memory space and access time.
   */
  Struct(CodeGen *codegen, std::string_view name, bool optimize_layout);

  /**
   * Add a member with the given name and type. The given name
   * @param name The name of the member.
   * @param type The type of the member.
   * @return A unique ID used to reference the member in the struct.
   */
  MemberId AddMember(std::string_view name, ast::Type *type);

  /**
   * Seal the struct, making it immutable. Calls to AddMember() will trigger an assertion.
   */
  void Seal();

  /**
   * @return True if the struct has been sealed; false otherwise.
   */
  bool IsSealed() const noexcept { return type_ != nullptr; }

  /**
   * @return The final name of the struct. This may be different than the name provided during
   *         instantiation to account for name collision. Use this name when referring to the
   *         struct during code generation. Using anything else may result in a compilation error.
   */
  ast::Identifier GetName() const noexcept { return name_; }

  /**
   * @return The final construct TPL type of the struct. This is only available after the struct
   *         has been sealed.
   */
  ast::Type *GetType() const noexcept { return type_; }

  /**
   * @return A pointer to the constructed TPL type. Like GetType(), this is only available after the
   *         structure has been sealed.
   */
  ast::Type *GetPtrToType() const { return ptr_to_type_; }

  /**
   * Generate a reference to the member with ID @em member_id in the struct pointed to by @em ptr.
   * The reference can be used as an L-Value or R-Value, depending on context, and can be used to
   * modify the value it references.
   *
   * @pre The TPL type of the input pointer must be a pointer to the TPL type of this structure.
   * @param ptr A pointer to the structure.
   * @param member_id The member_id to access.
   * @return A reference to the member in this structure with ID @em member_id.
   */
  ReferenceVT MemberGeneric(const ValueVT &ptr, MemberId member_id) const;

  /**
   * Generate a pointer to the member with ID @em member_id in the struct pointed to by @em ptr.
   * @pre The TPL type of the input pointer must be a pointer to the TPL type of this structure.
   * @param ptr A pointer to the structure.
   * @param member The member to access.
   * @return A pointer to the member in the structure pointed to by @em ptr.
   */
  ValueVT MemberPtrGeneric(const ValueVT &ptr, MemberId member) const;

  /**
   * Generate a reference to the member with ID @em member_id in the struct pointed to by @em ptr.
   * The reference can be used as an L-Value or R-Value, depending on context, and can be used to
   * modify the value it references.
   *
   * @pre The TPL type of the input pointer must be a pointer to the TPL type of this structure.
   * @tparam T The TPL type of the member.
   * @param ptr A pointer to the structure.
   * @param member_id The member_id to access.
   * @return A reference to the member in this structure with ID @em member_id.
   */
  template <typename T>
  Reference<T> Member(const ValueVT &ptr, MemberId member) const {
    return MemberGeneric(ptr, member).As<T>();
  }

  /**
   * Generate a pointer to the member with ID @em member_id in the struct pointed to by @em ptr.
   * @pre The TPL type of the input pointer must be a pointer to the TPL type of this structure.
   * @tparam T The TPL type of the member.
   * @param ptr A pointer to the structure.
   * @param member The member to access.
   * @return A pointer to the member in the structure pointed to by @em ptr.
   */
  template <typename T>
  Value<T *> MemberPtr(const ValueVT &ptr, MemberId member) const {
    return MemberPtrGeneric(ptr, member).As<T *>();
  }

  /**
   * @return A value representing the size of the final constructed type. This is only available
   *         after the type has been sealed through Seal().
   */
  Value<uint32_t> GetSize() const;

  /**
   * @return An EDSL value representing the byte offset of the member with the given ID @em member
   *         in the structure. This is only available after the type has been sealed through Seal().
   */
  Value<uint32_t> OffsetOf(MemberId member) const;

  /**
   * @return The size of the structure in bytes. This is only available after the structure has been
   *         sealed through Seal().
   */
  std::size_t GetSizeRaw() const noexcept;

  /**
   * @return The byte offset of the member with the given ID @em member in the structure. This is
   *         only available after the type has been sealed through Seal().
   */
  std::size_t OffsetOfRaw(MemberId member) const;

 private:
  // The code generator.
  CodeGen *const codegen_;
  // The final name of the struct.
  const ast::Identifier name_;
  // The members of the struct. This respects the original insertion order by
  // the caller and mustn't change. We use this to re-map IDs of members to
  // their names in the struct when generating member accesses.
  std::vector<ast::Field> members_;
  // The constructed type of the struct. Only set after Seal().
  ast::StructType *type_;
  ast::PointerType *ptr_to_type_;
  // Whether we should be optimizing the layout of the struct.
  const bool optimize_layout_;
};

}  // namespace tpl::sql::codegen::edsl

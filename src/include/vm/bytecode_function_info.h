#pragma once

#include <cstdint>
#include <vector>

#include "util/bitfield.h"
#include "util/common.h"
#include "util/macros.h"

namespace tpl {

namespace ast {
class Type;
}  // namespace ast

namespace vm {

using LocalId = u16;
using FunctionId = u16;

/// LocalInfo represents any local variable allocated in a function including
/// genuine local variable explicitly stated in the source, function parameters,
/// and temporary variables required for expression evaluation.
///
/// Locals have a fixed size, a static position (offset) in a function's
/// execution frame, and a static type. The virtual machine ensures that a
/// local variable's memory always has the correct alignment deemed by its type
/// and the machine architecture.
class LocalInfo {
 public:
  enum class Kind : u8 { Var, Parameter, Temporary };

  LocalInfo(u32 index, std::string name, ast::Type *type, u32 offset, Kind kind)
      : name_(std::move(name)),
        type_(type),
        offset_(offset),
        index_(index),
        kind_(kind) {}

  /// Return the size (in bytes) of this local variable
  /// \return The size in bytes
  u32 Size() const;

  // -------------------------------------------------------
  // Accessors Only
  // -------------------------------------------------------

  const std::string &name() const { return name_; }

  const ast::Type *type() const { return type_; }

  u32 id() const { return index_; }

  u32 offset() const { return offset_; }

  bool is_parameter() const { return kind_ == Kind::Parameter; }

 private:
  std::string name_;
  ast::Type *type_;
  u32 offset_;
  u32 index_;
  Kind kind_;
};

/// Local access encapsulates how a given local will be accessed by pairing a
/// local ID and an addressing mode.
class LocalVar {
 public:
  /// The different local addressing modes
  enum class AddressMode : u8 { Address = 0, Value = 1 };

  /// An invalid local variable
  LocalVar() : LocalVar(kInvalidOffset, AddressMode::Address) {}

  /// A local variable with a given addressing mode
  /// \param offset The byte-offset of the local variable in the function's
  /// execution/stack frame
  /// \param address_mode The addressing mode for this variable
  LocalVar(u32 offset, AddressMode address_mode)
      : bitfield_(AddressModeField::Encode(address_mode) |
                  LocalOffsetField::Encode(offset)) {}

  /// Return the addressing mode of for this local variable
  /// \return The addressing mode (direct or indirect) of this local
  AddressMode GetAddressMode() const {
    return AddressModeField::Decode(bitfield_);
  }

  /// Return the offset of this local variable in the function's execution frame
  /// \return The offset (in bytes) of this local in the function's frame
  u32 GetOffset() const { return LocalOffsetField::Decode(bitfield_); }

  /// Encode this local variable into an instruction stream
  /// \return The encoded value (and its addressing mode)
  u32 Encode() const { return bitfield_; }

  /// Decode the provided value from an instruction stream into a local variable
  /// that captures its offset and addressing more.
  /// \param encoded_var The encoded value of the variable
  /// \return The LocalVar representation
  static LocalVar Decode(u32 encoded_var) { return LocalVar(encoded_var); }

  /// Return a LocalVar that represents a dereferenced version of the local
  /// \return A "loaded" version of the variable
  LocalVar ValueOf() const { return LocalVar(GetOffset(), AddressMode::Value); }

  /// Is this a valid local variable?
  /// \return True if valid; false otherwise
  bool IsInvalid() const { return GetOffset() == kInvalidOffset; }

 private:
  // Single bit indicating the addressing mode of the local
  class AddressModeField : public util::BitField32<AddressMode, 0, 1> {};

  // The offset of the local variable in the function's execution frame
  class LocalOffsetField
      : public util::BitField32<u32, AddressModeField::kNextBit, 31> {};

  static const u32 kInvalidOffset = std::numeric_limits<u32>::max() >> 1;

 private:
  explicit LocalVar(u32 bitfield) : bitfield_(bitfield) {}

 private:
  u32 bitfield_;
};

/// A class that captures information about a function in the generated bytecode
class FunctionInfo {
  static const LocalId kRetVarOffset = 0;

 public:
  FunctionInfo(FunctionId id, std::string name)
      : id_(id),
        name_(std::move(name)),
        bytecode_range_(std::make_pair(0, 0)),
        frame_size_(0),
        num_params_(0),
        num_temps_(0) {}

  /// Allocate a new local variable of the given type and name. This returns a
  /// LocalVar object with the Address addressing mode (i.e., a pointer to the
  /// variable).
  /// @param type The TPL type of the variable
  /// @param name The name of the variable
  /// @return A pointer to the local variable encoded as a LocalVar
  LocalVar NewLocal(ast::Type *type, const std::string &name);

  /// Allocate a new function parameter.
  /// @param type The TPL type of the parameter
  /// @param name The name of the parameter
  /// @return A pointer to the local variable encoded as a LocalVar
  LocalVar NewParameterLocal(ast::Type *type, const std::string &name);

  /// Allocate a temporary function variable
  /// @param type The TPL type of the variable
  /// @return A pointer to the local variable encoded as a LocalVar
  LocalVar NewTempLocal(ast::Type *type);

  /// Lookup a local variable by name
  /// @param name The name of the local variable
  /// @return A pointer to the local variable encoded as a LocalVar
  LocalVar LookupLocal(const std::string &name) const;

  /// Return the ID of the return value for the function
  /// @return
  LocalVar GetReturnValueLocal() const {
    return LocalVar(kRetVarOffset, LocalVar::AddressMode::Address);
  }

  void MarkBytecodeRange(std::size_t start_offset, std::size_t end_offset) {
    TPL_ASSERT(start_offset < end_offset,
               "Starting offset must be smaller than ending offset");
    bytecode_range_ = std::make_pair(start_offset, end_offset);
  }

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  FunctionId id() const { return id_; }

  const std::string &name() const { return name_; }

  std::pair<std::size_t, std::size_t> bytecode_range() const {
    return bytecode_range_;
  }

  const std::vector<LocalInfo> &locals() const { return locals_; }

  std::size_t frame_size() const { return frame_size_; }

  u32 num_params() const { return num_params_; }

 private:
  // Allocate a new local variable in the function
  LocalVar NewLocal(ast::Type *type, const std::string &name,
                    LocalInfo::Kind kind);

  // Return the next available ID for a temporary variable
  u32 NextTempId() { return ++num_temps_; }

 private:
  FunctionId id_;
  std::string name_;
  std::pair<std::size_t, std::size_t> bytecode_range_;
  std::vector<LocalInfo> locals_;
  std::size_t frame_size_;

  u32 num_params_;
  u32 num_temps_;
};

}  // namespace vm
}  // namespace tpl
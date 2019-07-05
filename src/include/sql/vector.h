#pragma once

#include <bitset>
#include <iosfwd>
#include <memory>
#include <vector>

#include "sql/sql.h"
#include "util/common.h"
#include "util/macros.h"
#include "util/region.h"

namespace tpl::sql {

/**
 * A vector serves as the smallest unit of execution used by the execution
 * engine. It represents a contiguous chunk of values of a single type. Values
 * in a vector may not necessarily be SQL types; for example, we use vectors to
 * store pointers and hash values.
 *
 * A vector may own its data, reference data owned by another entity (e.g., the
 * base columns or data within another vector), or reference a single constant
 * value.
 *
 * A vector also has an optional selection vector containing indexes of the
 * valid elements in the vector. When a selection vector is available, it must
 * be used to access the vector's data since the vector may hold invalid data in
 * those positions (e.g., null pointers). An example of such indirect access:
 *
 * @code
 * u64 x = 0;
 * for (u64 i = 0; i < count; i++) {
 *   x += data[sel_vector[i]];
 * }
 * @endcode
 *
 * The selection vector is used primarily to activate and deactivate elements in
 * the vector.
 *
 * CAUTION: While there are methods to get/set individual vector elements, this
 * should be used very very sparingly. If you find yourself invoking this is in
 * a hot-loop, or very often, reconsider your interaction pattern with Vector,
 * and think about writing a new vector primitive to achieve your objective.
 */
class Vector {
  friend class VectorOps;
  friend class VectorProjectionIterator;

 public:
  using NullMask = std::bitset<kDefaultVectorSize>;

  static constexpr NullMask kZeroMask{0};

  /**
   * Create an empty vector.
   * @param type The type of the elements in the vector.
   */
  explicit Vector(TypeId type);

  /**
   * Create a non-owning vector that references the specified data.
   * @param type The primitive type ID of the elements in the vector.
   * @param data A pointer to the data.
   * @param count The number of elements in the vector
   */
  Vector(TypeId type, byte *data, u64 count);

  /**
   * Create a new owning vector that holds at most kDefaultVectorSize elements.
   * If the @em create_data flag is set, the vector will allocate data. If the
   * @em zero_data flag is set, the vector's data will be zeroed out.
   * @param type The primitive type ID of the elements in this vector.
   * @param create_data Should the vector actually allocate data?
   * @param clear Should the vector zero out the data if it allocates any?
   */
  Vector(TypeId type, bool create_data, bool clear);

  /**
   * Vector's cannot be implicitly copied.
   */
  DISALLOW_COPY(Vector);

  /**
   * Destructor.
   */
  ~Vector();

  /**
   * What is the type of the elements contained in this vector?
   */
  TypeId type_id() const { return type_; }

  /**
   * Return the number of elements in the vector.
   */
  u64 count() const { return count_; }

  /**
   * Set the count.
   */
  void set_count(u64 count) { count_ = count; }

  /**
   * Return the raw data pointer.
   */
  byte *data() const { return data_; }

  /**
   * Return the selection vector, NULL if there isn't one.
   */
  u32 *selection_vector() const { return sel_vector_; }

  /**
   * Return the NULL bitmask of elements in this vector.
   */
  const NullMask &null_mask() const { return null_mask_; }

  /**
   * Set the selection vector.
   */
  void SetSelectionVector(u32 *sel_vector, u64 count) {
    sel_vector_ = sel_vector;
    count_ = count;
  }

  /**
   * Is this vector holding a single constant value?
   */
  bool IsConstant() { return count_ == 1 && sel_vector_ == nullptr; }

  /**
   * Is the value at position @em index NULL?
   */
  bool IsNull(const u64 index) const {
    return null_mask_[sel_vector_ != nullptr ? sel_vector_[index] : index];
  }

  /**
   * Set the value at position @em index to @em null.
   */
  void SetNull(const u64 index, const bool null) {
    null_mask_[sel_vector_ != nullptr ? sel_vector_[index] : index] = null;
  }

  /**
   * Retrieve a pointer to the element at position @em index in the vector. If
   * the value is NULL, a null pointer is returned.
   *
   * This function will perform some checks to ensure the right type is
   * provided, but the checks are incomplete since some SQL types use the same
   * primitive underlying types. Be careful!
   *
   * @tparam T The type of the value.
   * @param index The position in the vector to read.
   * @return The element at the specified position, or NULL if the value is
   *         marked as such in the vector.
   */
  template <typename T>
  const T *GetValue(u64 index) const;

  /**
   * Return the string value of the element at the given index, or a NULL
   * pointer if the value at the index is NULL.
   * @param index The position in the vector to read.
   * @return The string at the index, or NULL if the value is marked NULL.
   */
  const char *GetStringValue(u64 index) const;

  /**
   * Set the value a position @em index in the vector to the value @em value.
   * This function will perform some checks to ensure the correct type is
   * provided, but the checks are incomplete. For example, it's possible to
   * store a BigInt value into a hash array because they both use unsigned
   * 64-bit primitive storage types. Be careful!
   *
   * @tparam T The type of the value.
   * @param index The position in the index to modify.
   * @param val The value to set in the vector.
   */
  template <typename T>
  void SetValue(u64 index, T val);

  /**
   * Set the value at position @em index in the vector to the given string
   * value @em val by copying the string.
   * @param index The index to set.
   * @param val The string value to copy into the vector.
   */
  void SetStringValue(u64 index, std::string_view val);

  /**
   * Cast this vector to a potentially different type.
   */
  void Cast(TypeId new_type);

  /**
   * Append the contents of vector @em other into this vector.
   */
  void Append(Vector &other);

  /**
   * Copies the contents of this vector into another vector. Callers can
   * optionally specify at what offset to begin copying at, but data is always
   * copied into the start of the destination vector. The default is 0.
   * @param other The vector to copy into.
   * @param offset The offset in this vector to begin copying.
   */
  void CopyTo(Vector *other, u64 offset = 0);

  /**
   * Move the data from this vector into another vector, and empty initialize
   * this vector.
   * @param other The vector that will take ownership of all our data, if any.
   */
  void MoveTo(Vector *other);

  /**
   * Flattens the vector, removing any selection vector.
   */
  void Flatten();

  /**
   *
   * @param type_id
   * @param data
   * @param nullmask
   * @param count
   */
  void Reference(TypeId type_id, byte *data, u32 *nullmask, u64 count);

  /**
   * Create a vector that references data held (and owned!) by another vector.
   * @param other The vector to reference.
   */
  void Reference(Vector *other);

  /**
   * Return a string representation of this vector.
   */
  std::string ToString() const;

  /**
   * Print a string representation of this vector to the output stream.
   */
  void Dump(std::ostream &os) const;

 private:
  // Create a new vector with the specified type. Any existing data is
  // destroyed.
  void Initialize(TypeId new_type, bool clear);

  // Destroy the vector, delete any owned data and reset it to an empty vector.
  void Destroy();

 private:
  // Container for all strings this vector owns and contains
  class Strings {
   public:
    // Construct
    Strings();

    // Move constructor
    Strings(Strings &&) = default;

    // No copying
    DISALLOW_COPY(Strings);

    // Move assignment
    Strings &operator=(Strings &&) = default;

    // Return the number of strings are in this container
    u32 GetNumStrings() const { return num_strings_; }

    // Copy the given string into this container, returning a pointer to it.
    char *AddString(std::string_view str);

    // Deallocate all memory in this container
    void Destroy();

   private:
    // Where the strings live
    util::Region region_;
    // Number of strings
    u32 num_strings_;
  };

 protected:
  // If the vector holds allocated data, this field manages it.
  std::unique_ptr<byte[]> owned_data_;

 private:
  // The type of the elements stored in the vector.
  TypeId type_;
  // The number of elements in the vector.
  u64 count_;
  // A pointer to the data.
  byte *data_;
  // The selection vector of the vector.
  u32 *sel_vector_;
  // The null mask used to indicate if an element in the vector is NULL.
  NullMask null_mask_;
  // String container
  Strings strings_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

// Reading non-string values
template <typename T>
inline const T *Vector::GetValue(const u64 index) const {
  TPL_ASSERT(type_ == GetTypeId<T>(),
             "Attempt to access element from vector with different type");
  if (IsNull(index)) {
    return nullptr;
  }
  auto actual_index = (sel_vector_ != nullptr ? sel_vector_[index] : index);
  return reinterpret_cast<T *>(data_) + actual_index;
}

inline const char *Vector::GetStringValue(u64 index) const {
  TPL_ASSERT(type_ == TypeId::Varchar,
             "Attempt to access element from vector with different type");
  if (IsNull(index)) {
    return nullptr;
  }
  auto actual_index = (sel_vector_ != nullptr ? sel_vector_[index] : index);
  return reinterpret_cast<const char **>(data_)[actual_index];
}

// Non-string set
template <typename T>
inline void Vector::SetValue(const u64 index, const T val) {
  TPL_ASSERT(IsTypeFixedSize(type_), "Setting value must be fixed length type");
  TPL_ASSERT(type_ == GetTypeId<T>(), "Mismatched types");
  TPL_ASSERT(index < count_, "Out-of-range access");
  auto actual_index = (sel_vector_ != nullptr ? sel_vector_[index] : index);
  reinterpret_cast<T *>(data_)[actual_index] = val;
}

inline void Vector::SetStringValue(const u64 index,
                                   const std::string_view val) {
  TPL_ASSERT(type_ == TypeId::Varchar, "Setting string in non-string vector");
  TPL_ASSERT(index < count_, "Out-of-range access");
  auto actual_index = (sel_vector_ != nullptr ? sel_vector_[index] : index);
  reinterpret_cast<const char **>(data_)[actual_index] =
      strings_.AddString(val);
}

}  // namespace tpl::sql

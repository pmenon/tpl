#pragma once

#include <bitset>
#include <iosfwd>
#include <memory>
#include <string>

#include "sql/generic_value.h"
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
  sel_t *selection_vector() const { return sel_vector_; }

  /**
   * Return the NULL bitmask of elements in this vector.
   */
  const NullMask &null_mask() const { return null_mask_; }

  /**
   * Set the mask.
   */
  void set_null_mask(const NullMask &other) { null_mask_ = other; }

  /**
   * Set the mask to the provided mask.
   */
  void set_null_mask(NullMask &&other) { null_mask_ = other; }

  /**
   * Set the selection vector.
   */
  void SetSelectionVector(sel_t *sel_vector, u64 count) {
    sel_vector_ = sel_vector;
    count_ = count;
  }

  /**
   * Is this vector holding a single constant value?
   */
  bool IsConstant() const { return count_ == 1 && sel_vector_ == nullptr; }

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
   * Returns the value of the element at the given position in the vector.
   *
   * NOTE: This shouldn't be used in performance-critial code. It's mostly for
   * debugging and validity checks, or for read-once-per-vector type operations.
   *
   * @param index The position in the vector to read.
   * @return The element at the specified position.
   */
  GenericValue GetValue(u64 index) const;

  /**
   * Set the value a position @em index in the vector to the value @em value.
   *
   * NOTE: This shouldn't be used in performance-critial code. It's mostly for
   * debugging and validity checks, or for read-once-per-vector type operations.
   *
   * @param index The position in the index to modify.
   * @param val The value to set in the vector.
   */
  void SetValue(u64 index, const GenericValue &val);

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
   * Reference a single value.
   * @param value The value to reference.
   */
  void Reference(GenericValue *value);

  /**
   * Reference a specific chunk of data.
   * @param type_id The ID of the type.
   * @param data The data.
   * @param nullmask The NULL bitmap.
   * @param count The number of elements in the array.
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
  sel_t *sel_vector_;
  // The null mask used to indicate if an element in the vector is NULL.
  NullMask null_mask_;
  // String container
  Strings strings_;
};

}  // namespace tpl::sql
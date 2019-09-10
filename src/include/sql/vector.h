#pragma once

#include <bitset>
#include <iosfwd>
#include <memory>
#include <string>
#include <utility>

#include "common/common.h"
#include "common/macros.h"
#include "sql/generic_value.h"
#include "sql/sql.h"
#include "util/bit_vector.h"
#include "util/region.h"

namespace tpl::sql {

/**
 * A vector represents a contiguous chunk of values of a single type. A vector may (1) own its data,
 * or (2) reference data owned by another entity, e.g., base table column data, data within another
 * (intermediate) vector, or a constant value.
 *
 * A vector also has an optional selection vector containing the indexes of the valid elements in
 * the vector. When a selection vector is available, it must be used to access the vector's data
 * since the vector may hold invalid data in unselected positions (e.g., null pointers). This
 * functionality is provided for you through @em VectorOps::Exec(), but can be done manually as the
 * below example illustrates:
 *
 * @code
 * uint64_t x = 0;
 * for (uint64_t i = 0; i < count; i++) {
 *   x += data_[sel_vector_[i]];
 * }
 * @endcode
 *
 * The selection vector is used primarily to activate and deactivate elements in the vector without
 * copying or moving data.
 *
 * Vectors have a maximum capacity determined by the global constant @em kDefaultVectorSize usually
 * set to 2048 elements.
 *
 * CAUTION: While there are methods to get/set individual vector elements, this should be used
 * sparingly. If you find yourself invoking this is in a hot-loop, or very often, reconsider your
 * interaction pattern with Vector, and think about writing a new vector primitive to achieve your
 * objective.
 *
 * Inspired by VectorWise.
 */
class Vector {
  friend class VectorOps;
  friend class VectorProjectionIterator;

 public:
  using NullMask = util::BitVector<uint64_t>;

  /**
   * Create an empty vector.
   * @param type The type of the elements in the vector.
   */
  explicit Vector(TypeId type);

  /**
   * Create a new owning vector with a given size (at most kDefaultVectorSize elements). If the
   * @em clear flag is set, the vector's data will be zeroed out.
   * @param type The primitive type ID of the elements in this vector.
   * @param count The size of the vector.
   * @param clear Should the vector zero out the data if it allocates any?
   */
  Vector(TypeId type, uint64_t count, bool clear);

  /**
   * Create a non-owning vector that references the specified data.
   * @param type The primitive type ID of the elements in the vector.
   * @param data A pointer to the data.
   * @param count The number of elements in the vector
   */
  Vector(TypeId type, byte *data, uint64_t count);

  /**
   * Vector's cannot be implicitly copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(Vector);

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
  uint64_t count() const { return count_; }

  /**
   * Set the count.
   */
  void set_count(uint64_t count) { count_ = count; }

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
   * Return a mutable instance of the NULL bitmask in this vector.
   */
  NullMask *mutable_null_mask() { return &null_mask_; }

  /**
   * Set the selection vector.
   */
  void SetSelectionVector(sel_t *const sel_vector, const uint64_t count) {
    sel_vector_ = sel_vector;
    count_ = count;
  }

  /**
   * Is this vector holding a single constant value?
   */
  bool IsConstant() const { return count_ == 1 && sel_vector_ == nullptr; }

  /**
   * Compute the selectivity, i.e., the fraction of tuples that are externally visible.
   */
  double ComputeSelectivity() const {
    return IsConstant() ? 1.0 : static_cast<double>(count_) / kDefaultVectorSize;
  }

  /**
   * Is the value at position @em index NULL?
   */
  bool IsNull(const uint64_t index) const {
    return null_mask_[sel_vector_ != nullptr ? sel_vector_[index] : index];
  }

  /**
   * Set the value at position @em index to @em null.
   */
  void SetNull(const uint64_t index, const bool null) {
    null_mask_[sel_vector_ != nullptr ? sel_vector_[index] : index] = null;
  }

  /**
   * Returns the value of the element at the given position in the vector.
   *
   * NOTE: This shouldn't be used in performance-critical code. It's mostly for
   * debugging and validity checks, or for read-once-per-vector type operations.
   *
   * @param index The position in the vector to read.
   * @return The element at the specified position.
   */
  GenericValue GetValue(uint64_t index) const;

  /**
   * Set the value at position @em index in the vector to the value @em value.
   *
   * NOTE: This shouldn't be used in performance-critical code. It's mostly for
   * debugging and validity checks, or for read-once-per-vector type operations.
   *
   * @param index The (zero-based) index in the element to modify.
   * @param val The value to set the element to.
   */
  void SetValue(uint64_t index, const GenericValue &val);

  /**
   * Cast this vector to a different type. If the target type is the same as the
   * current type, nothing is done.
   */
  void Cast(TypeId new_type);

  /**
   * Append the contents of the provided vector @em other into this vector.
   */
  void Append(Vector &other);

  /**
   * Copies the contents of this vector into another vector. Callers can
   * optionally specify at what offset to begin copying at, but data is always
   * copied into the start of the destination vector. The default is 0.
   * @param other The vector to copy into.
   * @param offset The offset in this vector to begin copying.
   */
  void CopyTo(Vector *other, uint64_t offset = 0);

  /**
   * Move the data from this vector into another vector, and empty initialize
   * this vector.
   * @param other The vector that will take ownership of all our data, if any.
   */
  void MoveTo(Vector *other);

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
  void Reference(TypeId type_id, byte *data, uint32_t *nullmask, uint64_t count);

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
  void Dump(std::ostream &stream) const;

  /**
   * Perform an integrity check on this vector instance. This is used in debug
   * mode for sanity checks.
   */
  void CheckIntegrity() const;

 private:
  // Create a new vector with the provided type. Any existing data is destroyed.
  void Initialize(TypeId new_type, bool clear);

  // Destroy the vector, delete any owned data, and reset it to an empty vector.
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
    uint32_t GetNumStrings() const { return num_strings_; }

    // Copy the given string into this container, returning a pointer to it.
    char *AddString(std::string_view str);

    // Deallocate all memory in this container
    void Destroy();

   private:
    // Where the strings live
    util::Region region_;
    // Number of strings
    uint32_t num_strings_;
  };

 private:
  // The type of the elements stored in the vector.
  TypeId type_;
  // The number of elements in the vector.
  uint64_t count_;
  // A pointer to the data.
  byte *data_;
  // The selection vector of the vector.
  sel_t *sel_vector_;
  // The null mask used to indicate if an element in the vector is NULL.
  NullMask null_mask_;
  // String container
  Strings strings_;
  // If the vector holds allocated data, this field manages it.
  std::unique_ptr<byte[]> owned_data_;
};

}  // namespace tpl::sql

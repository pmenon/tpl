#pragma once

#include <iosfwd>
#include <memory>
#include <string>
#include <utility>

#include "common/common.h"
#include "common/macros.h"
#include "sql/generic_value.h"
#include "sql/sql.h"
#include "util/bit_vector.h"
#include "util/string_heap.h"

namespace tpl::sql {

/**
 * A vector represents a contiguous chunk of values of a single type. A vector may allocate and own
 * its data, or <b>reference</b> data owned by some other entity, e.g., base table column data, data
 * within another vector, or a constant value.
 *
 * All vectors have a maximum capacity (see Vector::GetCapacity()) determined by the global constant
 * ::tpl::kDefaultVectorSize usually set to 2048 elements. Vectors also have a <b>size</b> (see
 * Vector::GetSize()) that reflects the number of physically contiguous elements <b>currently</b> in
 * the vector. A vector's size can fluctuate through its life, but will always be less than its
 * capacity. Finally, a vector has an <b>active count</b> (see Vector::GetCount()) that represents
 * the number of externally visible elements. Elements may become inactive if they have been
 * filtered out through predicates. The visibility of elements in the vector is controlled through
 * a <b>selection vector</b>.
 *
 * A selection vector is an array containing the indexes of the <i>active</i> vector elements. When
 * a selection vector is available, it must be used to access the vector's data since the vector may
 * hold otherwise invalid data in unselected positions (e.g., null pointers). This functionality is
 * provided for you through VectorOps::Exec(), but can be done manually as the below example
 * illustrates:
 *
 * @code
 * Vector vec ...
 * sel_t *sel_vec = vec.selection_vector();
 * uint64_t x = 0;
 * for (uint64_t i = 0; i < vec.GetCount(); i++) {
 *   x += vec.data()[sel_vec[i]];
 * }
 * @endcode
 *
 * The selection vector is used primarily to activate and deactivate elements in the vector without
 * copying or moving data. In general, the active count is <= the size which is <= the capacity. If
 * there is no selection vector, the active element count and size will match. Otherwise, the active
 * element count equals the size of the selection vector.
 *
 * <b>Usage</b>:
 *
 * Referencing-vectors are created with <b>zero</b> capacity. No memory is allocated and the vector
 * can only ever reference external data.
 *
 * To create a referencing vector whose data is only available <b>after</b> construction:
 * @code
 * Vector vec(TypeId::SmallInt);
 * ...
 * vec.Reset(...);
 * @endcode
 *
 * To create a referencing vector whose external data is immediately available:
 * @code
 * Vector vec(TypeId::Integer, data, size);
 * // Vector size (and count) match the size provide at construction time
 * auto first = vec.GetValue(0);
 * ...
 * @endcode
 *
 * Both the above approaches set the size of the vector based on the size of the underlying data.
 * Referencing-vectors enable users to lift externally stored data into the vector-processing
 * ecosystem; the rich library of vector operations in tpl::sql::VectorOps can be executed on native
 * arrays.
 *
 * Owning vectors are created empty and must be explicitly resized after construction:
 * @code
 * Vector vec(TypeId::Double, true, false);
 * vec.Resize(100);
 * vec->SetNull(10, true);
 * // ...
 * @endcode
 *
 * <b>Caution</b>:
 *
 * While there are methods to get/set individual vector elements, this should be used
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
   * Create a new owning vector with a maximum capacity of kDefaultVectorSize. If @em create_data
   * is set, the vector will allocate memory for the vector's contents. If @em clear is set, the
   * memory will be zeroed out after allocation.
   * @param type The primitive type ID of the elements in this vector.
   * @param create_data Should the vector allocate space for the contents?
   * @param clear Should the vector zero the data if it allocates any?
   */
  Vector(TypeId type, bool create_data, bool clear);

  /**
   * Create a non-owning vector that references the specified data.
   * @param type The primitive type ID of the elements in the vector.
   * @param data A pointer to the data.
   * @param size The number of elements in the vector
   */
  Vector(TypeId type, byte *data, uint64_t size);

  /**
   * Vector's cannot be implicitly copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(Vector);

  /**
   * Destructor.
   */
  ~Vector();

  /**
   * @return The type of the elements contained in this vector.
   */
  TypeId GetTypeId() const noexcept { return type_; }

  /**
   * @return The number of active, i.e., externally visible, elements in the vector. Active elements
   *         are those that have survived any filters in the selection vector. The count of a vector
   *         is guaranteed to be <= the size of the vector.
   */
  uint64_t GetCount() const noexcept { return count_; }

  /**
   * @return The total number of tuples currently in the vector, including those that may have been
   *         filtered out by the selection vector, if one exists. The size of a vector is always
   *         greater than or equal to the selected count.
   */
  uint64_t GetSize() const noexcept { return num_elems_; }

  /**
   * @return The maximum capacity of this vector.
   */
  uint64_t GetCapacity() const noexcept { return kDefaultVectorSize; }

  /**
   * @return The raw untyped data pointer.
   */
  byte *GetData() const noexcept { return data_; }

  /**
   * @return The selection vector; NULL if there isn't one.
   */
  sel_t *GetSelectionVector() const noexcept { return sel_vector_; }

  /**
   * @return An immutable view of this vector's NULL bitmask. This bitmask positionally indicates
   *         which elements in the vector are considered NULL.
   */
  const NullMask &GetNullMask() const noexcept { return null_mask_; }

  /**
   * @return A mutable pointer to this vector's NULL bitmask.
   */
  NullMask *GetMutableNullMask() noexcept { return &null_mask_; }

  /**
   * @return A mutable pointer to this vector's string heap.
   */
  VarlenHeap *GetMutableStringHeap() noexcept { return &varlens_; }

  /**
   * Set the selection vector.
   * @param sel_vector The selection vector.
   * @param count The number of elements in the selection vector.
   */
  void SetSelectionVector(sel_t *const sel_vector, const uint64_t count) {
    TPL_ASSERT(count <= num_elems_, "Selection vector count cannot exceed vector size");
    sel_vector_ = sel_vector;
    count_ = count;
  }

  /**
   * @return True if this vector is holding a single constant value; false otherwise.
   */
  bool IsConstant() const noexcept { return num_elems_ == 1 && sel_vector_ == nullptr; }

  /**
   * @return True if this vector is empty; false otherwise.
   */
  bool IsEmpty() const noexcept { return num_elems_ == 0; }

  /**
   * @return The computed selectivity of this vector, i.e., the fraction of tuples that are
   *         externally visible.
   */
  float ComputeSelectivity() const noexcept {
    return IsEmpty() ? 0 : static_cast<float>(count_) / num_elems_;
  }

  /**
   * @return True if the value at index @em index is NULL; false otherwise.
   */
  bool IsNull(const uint64_t index) const {
    return null_mask_[sel_vector_ != nullptr ? sel_vector_[index] : index];
  }

  /**
   * Set the value at position @em index to @em null.
   * @param index The index of the element to modify.
   * @param null Whether the element is NULL.
   */
  void SetNull(const uint64_t index, const bool null) {
    null_mask_[sel_vector_ != nullptr ? sel_vector_[index] : index] = null;
  }

  /**
   * Returns the value of the element at the given position in the vector.
   *
   * NOTE: This shouldn't be used in performance-critical code. It's mostly for debugging and
   * validity checks, or for read-once-per-vector type operations.
   *
   * @param index The position in the vector to read.
   * @return The element at the specified position.
   */
  GenericValue GetValue(uint64_t index) const;

  /**
   * Set the value at position @em index in the vector to the value @em value.
   *
   * NOTE: This shouldn't be used in performance-critical code. It's mostly for debugging and
   * validity checks, or for read-once-per-vector type operations.
   *
   * @param index The (zero-based) index in the element to modify.
   * @param val The value to set the element to.
   */
  void SetValue(uint64_t index, const GenericValue &val);

  /**
   * Resize the vector to the given size. Resizing REMOVES any existing selection vector, reverts
   * the selected count and total count to the provided size.
   *
   * @pre The new size must be less than the capacity.
   *
   * @param size The size to set the vector to.
   */
  void Resize(uint32_t size);

  /**
   * Cast this vector to a different type. If the target type is the same as the current type,
   * nothing is done.
   * @param new_type The type to cast this vector into.
   */
  void Cast(TypeId new_type);

  /**
   * Append the contents of the provided vector @em other into this vector.
   * @param other The vector whose contents will be copied and appended to the end of this vector.
   */
  void Append(const Vector &other);

  /**
   * Copies the contents of this vector into another vector. Callers can optionally specify at what
   * offset to begin copying at, but data is always copied into the start of the destination vector.
   * The default is 0.
   *
   * @param other The vector to copy into.
   * @param offset The offset in this vector to begin copying.
   */
  void CopyTo(Vector *other, uint64_t offset = 0);

  /**
   * Move the data from this vector into another vector, and empty initialize this vector.
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
   * @param data The data.
   * @param nullmask The NULL bitmap.
   * @param size The number of elements in the array.
   */
  void Reference(byte *data, uint32_t *nullmask, uint64_t size);

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
   * @param The output stream.
   */
  void Dump(std::ostream &stream) const;

  /**
   * Perform an integrity check on this vector. This is used in debug mode for sanity checks.
   */
  void CheckIntegrity() const;

 private:
  // Create a new vector with the provided type. Any existing data is destroyed.
  void Initialize(TypeId new_type, bool clear);

  // Destroy the vector, delete any owned data, and reset it to an empty vector.
  void Destroy();

 private:
  // The type of the elements stored in the vector
  TypeId type_;

  // The number of elements in the vector
  uint64_t count_;

  // The number of physically contiguous elements in the vector
  uint64_t num_elems_;

  // A pointer to the data.
  byte *data_;

  // The selection vector of the vector
  sel_t *sel_vector_;

  // The null mask used to indicate if an element in the vector is NULL
  NullMask null_mask_;

  // Heap container for strings owned by this vector
  VarlenHeap varlens_;

  // If the vector holds allocated data, this field manages it
  std::unique_ptr<byte[]> owned_data_;
};

}  // namespace tpl::sql

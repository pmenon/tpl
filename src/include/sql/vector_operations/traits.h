#pragma once

namespace tpl::sql {

class TupleIdList;

namespace traits {

/**
 * A struct used to determine if a given operation on a given type should be optimized to use a
 * full-computation implementation. By default, full-computation is disabled.
 *
 * If you want to enable full-computation for your operation, specialize this struct.
 *
 * @tparam T The types of the vector elements.
 * @tparam Op The operation.
 * @tparam Enable
 */
template <typename T, typename Op, typename Enable = void>
struct ShouldPerformFullCompute {
  /**
   * Call operator accepting a potentially null list of TIDs on which to perform the given templated
   * operation.
   * @param tid_list Potentially null filtered TID list.
   * @return True if full-computation should be performed; false otherwise.
   */
  bool operator()(const TupleIdList *tid_list) { return false; }
};

}  // namespace traits

}  // namespace tpl::sql

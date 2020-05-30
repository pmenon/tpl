#pragma once

#include <type_traits>
#include <utility>

#include "common/common.h"
#include "sql/vector.h"
#include "sql/vector_operations/traits.h"
#include "sql/vector_operations/vector_operations.h"

namespace tpl::sql {

/**
 * Utility class to perform three-way ternary operations on vectors.
 */
class TernaryOperationExecutor : public AllStatic {
 public:
  /**
   * Execute the templated ternary operation, @em op, on all active elements contained in three
   * input vectors, @em a, @em b, and @em c, and store the result into an output vector, @em result.
   *
   * @pre The template types of both inputs and the output match the underlying vector types.
   * @post The output vector has the same shape (size and filter status) as the largest input.
   *
   * @note This function leverages the tpl::sql::traits::ShouldPerformFullCompute trait to determine
   *       whether the operation should be performed on ALL vector elements or just the active
   *       elements. Callers can control this feature by optionally specialization the trait for
   *       their operation type. If you want to use this optimization, you cannot pass in a
   *       std::function; move your logic into a function object and pass an instance.
   *
   * @tparam AType The native CPP type of the elements in the first input vector.
   * @tparam BType The native CPP type of the elements in the second input vector.
   * @tparam CType The native CPP type of the elements in the third input vector.
   * @tparam ResultType The native CPP type of the elements in the result vector.
   * @tparam Op The three-argument functor.
   * @tparam IgnoreNull Flag indicating if the operation should skip NULL values as in either input.
   * @param a The first input vector argument.
   * @param b The second input vector argument.
   * @param c The third input vector argument.
   * @param[out] result The output vector.
   */
  template <typename AType, typename BType, typename CType, typename ResultType, typename Op,
            bool IgnoreNull = false>
  static void Execute(const Vector &a, const Vector &b, const Vector &c, Vector *result) {
    Execute<AType, BType, CType, ResultType, Op, IgnoreNull>(a, b, c, result, Op{});
  }

  /**
   * Execute the provided ternary operation, @em op, on all active elements contained in three
   * input vectors, @em a, @em b, and @em c, and store the result into an output vector, @em result.
   *
   * @pre The template types of both inputs and the output match the underlying vector types.
   * @post The output vector has the same shape (size and filter status) as the largest input.
   *
   * @note This function leverages the tpl::sql::traits::ShouldPerformFullCompute trait to determine
   *       whether the operation should be performed on ALL vector elements or just the active
   *       elements. Callers can control this feature by optionally specialization the trait for
   *       their operation type. If you want to use this optimization, you cannot pass in a
   *       std::function; move your logic into a function object and pass an instance.
   *
   * @tparam AType The native CPP type of the elements in the first input vector.
   * @tparam BType The native CPP type of the elements in the second input vector.
   * @tparam CType The native CPP type of the elements in the third input vector.
   * @tparam ResultType The native CPP type of the elements in the result vector.
   * @tparam Op The three-argument functor.
   * @tparam IgnoreNull Flag indicating if the operation should skip NULL values as in either input.
   * @param a The first input vector argument.
   * @param b The second input vector argument.
   * @param c The third input vector argument.
   * @param[out] result The output vector.
   */
  template <typename AType, typename BType, typename CType, typename ResultType, typename Op,
            bool IgnoreNull = false>
  static void Execute(const Vector &a, const Vector &b, const Vector &c, Vector *result, Op op) {
    GetVectorIndexer(a, [&](auto a_indexer) {
      GetVectorIndexer(b, [&](auto b_indexer) {
        GetVectorIndexer(c, [&](auto c_indexer) {
          ExecuteImpl<AType, BType, CType, ResultType, Op, IgnoreNull>(a, b, c, result, a_indexer,
                                                                       b_indexer, c_indexer, op);
        });
      });
    });
  }

  /**
   * Evaluate the templated ternary comparison function, @em Op, on elements in @em a, @em b, and
   * @em c with TIDs in @em tid_list, retaining only those TIDs that for which the comparison
   * returns true.
   *
   * @pre 1. The template types of both inputs match the underlying vector types.
   *      2. The comparison operator must be callable as: bool(AType,BType,CType)
   *      3. The TID list must not be larger than the largest input's vector capacity.
   *
   * @note This function leverages the tpl::sql::traits::ShouldPerformFullCompute trait to determine
   *       whether the operation should be performed on ALL vector elements or just the active
   *       elements. Callers can control this feature by optionally specialization the trait for
   *       their operation type.
   *
   * @tparam AType The native CPP type of the elements in the first input vector.
   * @tparam BType The native CPP type of the elements in the second input vector.
   * @tparam CType The native CPP type of the elements in the third input vector.
   * @tparam Op The three-argument functor.
   * @param a The first input vector argument.
   * @param b The second input vector argument.
   * @param c The third input vector argument.
   * @param[in,out] tid_list The list of TIDs to operate on and filter.
   */
  template <typename AType, typename BType, typename CType, typename Op>
  static void Select(const Vector &a, const Vector &b, const Vector &c, TupleIdList *tid_list) {
    Select<AType, BType, CType, Op>(a, b, c, tid_list, Op{});
  }

  /**
   * Evaluate the provided ternary comparison function, @em Op, on elements in @em a, @em b, and
   * @em c with TIDs in @em tid_list, retaining only those TIDs that for which the comparison
   * returns true.
   *
   * @pre 1. The template types of both inputs match the underlying vector types.
   *      2. The comparison operator must be callable as: bool(AType,BType,CType)
   *      3. The TID list must not be larger than the largest input's vector capacity.
   *
   * @note This function leverages the tpl::sql::traits::ShouldPerformFullCompute trait to determine
   *       whether the operation should be performed on ALL vector elements or just the active
   *       elements. Callers can control this feature by optionally specialization the trait for
   *       their operation type.
   *
   * @tparam AType The native CPP type of the elements in the first input vector.
   * @tparam BType The native CPP type of the elements in the second input vector.
   * @tparam CType The native CPP type of the elements in the third input vector.
   * @tparam Op The three-argument functor.
   * @param a The first input vector argument.
   * @param b The second input vector argument.
   * @param c The third input vector argument.
   * @param[in,out] tid_list The list of TIDs to operate on and filter.
   */
  template <typename AType, typename BType, typename CType, typename Op>
  static void Select(const Vector &a, const Vector &b, const Vector &c, TupleIdList *tid_list,
                     Op op) {
    if ((a.IsConstant() && a.IsNull(0)) || (b.IsConstant() && b.IsNull(0)) ||
        (c.IsConstant() && c.IsNull(0))) {
      tid_list->Clear();
      return;
    }

    GetVectorIndexer(a, [&](auto a_indexer) {
      GetVectorIndexer(b, [&](auto b_indexer) {
        GetVectorIndexer(c, [&](auto c_indexer) {
          SelectImpl<AType, BType, CType, Op>(a, b, c, a_indexer, b_indexer, c_indexer, tid_list,
                                              op);
        });
      });
    });
  }

 private:
  struct Identity {
    template <typename T>
    T operator()(T t) {
      return t;
    }
  };

  struct Zero {
    template <typename T>
    T operator()(T) {
      return 0;
    }
  };

  template <typename Callback>
  static void GetVectorIndexer(const Vector &input, Callback cb) {
    if (input.IsConstant()) {
      cb(Zero{});
    } else {
      cb(Identity{});
    }
  }

  template <typename AType, typename BType, typename CType, typename ResultType, typename Op,
            bool IgnoreNull, typename AIndexer, typename BIndexer, typename CIndexer>
  static void ExecuteImpl(const Vector &a, const Vector &b, const Vector &c, AIndexer a_indexer,
                          BIndexer b_indexer, CIndexer c_indexer, Vector *result, Op op) {
    TPL_ASSERT(a.GetCount() == b.GetCount(), "Mismatched vector counts for comparison");
    TPL_ASSERT(a.GetCount() == c.GetCount(), "Mismatched vector counts for comparison");

    auto *RESTRICT a_data = reinterpret_cast<AType *>(a.GetData());
    auto *RESTRICT b_data = reinterpret_cast<BType *>(b.GetData());
    auto *RESTRICT c_data = reinterpret_cast<CType *>(c.GetData());
    auto *RESTRICT result_data = reinterpret_cast<ResultType *>(result->GetData());

    result->Resize(a.GetSize());
    if (!a.IsConstant()) result->GetMutableNullMask()->Copy(a.GetNullMask());
    if (!b.IsConstant()) result->GetMutableNullMask()->Copy(b.GetNullMask());
    if (!c.IsConstant()) result->GetMutableNullMask()->Copy(c.GetNullMask());
    result->SetFilteredTupleIdList(a.GetFilteredTupleIdList(), a.GetCount());

    if (IgnoreNull && result->GetNullMask().Any()) {
      VectorOps::Exec(a, [&](uint64_t i, uint64_t k) {
        const auto a_idx = a_indexer(i);
        const auto b_idx = b_indexer(i);
        const auto c_idx = c_indexer(i);
        if (!result->GetNullMask()[i]) {
          result_data[i] = op(a_data[a_idx], b_data[b_idx], c_data[c_idx]);
        }
      });
    } else {
      if (traits::ShouldPerformFullCompute<Op>()(a.GetFilteredTupleIdList())) {
        VectorOps::ExecIgnoreFilter(a, [&](uint64_t i, uint64_t k) {
          const auto a_idx = a_indexer(i);
          const auto b_idx = b_indexer(i);
          const auto c_idx = c_indexer(i);
          result_data[i] = op(a_data[a_idx], b_data[b_idx], c_data[c_idx]);
        });
      } else {
        VectorOps::Exec(a, [&](uint64_t i, uint64_t k) {
          const auto a_idx = a_indexer(i);
          const auto b_idx = b_indexer(i);
          const auto c_idx = c_indexer(i);
          result_data[i] = op(a_data[a_idx], b_data[b_idx], c_data[c_idx]);
        });
      }
    }
  }

  template <typename AType, typename BType, typename CType, typename Op, typename AIndexer,
            typename BIndexer, typename CIndexer>
  static void SelectImpl(const Vector &a, const Vector &b, const Vector &c, AIndexer a_indexer,
                         BIndexer b_indexer, CIndexer c_indexer, TupleIdList *tid_list, Op op) {
    auto *RESTRICT a_data = reinterpret_cast<const AType *>(a.GetData());
    auto *RESTRICT b_data = reinterpret_cast<const BType *>(b.GetData());
    auto *RESTRICT c_data = reinterpret_cast<const CType *>(c.GetData());

    if (traits::ShouldPerformFullCompute<Op>()(a.GetFilteredTupleIdList())) {
      // Safe full-compute. Refer to comment at start of file for explanation.
      TupleIdList::BitVectorType *bit_vector = tid_list->GetMutableBits();
      // Perform blind selection.
      bit_vector->UpdateFull([&](uint64_t i) {
        const auto a_idx = a_indexer(i);
        const auto b_idx = b_indexer(i);
        const auto c_idx = c_indexer(i);
        return op(a_data[a_idx], b_data[b_idx], c_data[c_idx]);
      });
      // Strip nulls.
      if (!a.IsConstant()) bit_vector->Difference(a.GetNullMask());
      if (!b.IsConstant()) bit_vector->Difference(b.GetNullMask());
      if (!c.IsConstant()) bit_vector->Difference(c.GetNullMask());
      return;
    }

    // Remove all NULL entries in any vector.
    if (!a.IsConstant()) tid_list->GetMutableBits()->Difference(a.GetNullMask());
    if (!b.IsConstant()) tid_list->GetMutableBits()->Difference(b.GetNullMask());
    if (!c.IsConstant()) tid_list->GetMutableBits()->Difference(c.GetNullMask());

    // Filter!
    tid_list->Filter([&](uint64_t i) {
      const auto a_idx = a_indexer(i);
      const auto b_idx = b_indexer(i);
      const auto c_idx = c_indexer(i);
      return op(a_data[a_idx], b_data[b_idx], c_data[c_idx]);
    });
  }
};

}  // namespace tpl::sql

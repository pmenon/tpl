#include "sql/vector_operations/vector_operators.h"

#include <string>

#include "common/exception.h"
#include "sql/operations/cast_operators.h"

namespace tpl::sql {

namespace {

template <typename SrcT, typename DestT, typename Op>
void CastFromSrcTypeToDestType(const Vector &source, Vector *target) {
  auto src_data = reinterpret_cast<SrcT *>(source.GetData());
  auto target_data = reinterpret_cast<DestT *>(target->GetData());
  if (source.GetNullMask().Any()) {
    // Slow-path need to check NULLs
    VectorOps::Exec(source, [&](uint64_t i, uint64_t k) {
      if (!source.GetNullMask()[i]) {
        target_data[i] = Op::template Apply<SrcT, DestT>(src_data[i]);
      }
    });
  } else {
    // Fast path: no nulls in source vector, direct cast
    VectorOps::Exec(source, [&](uint64_t i, uint64_t k) {
      target_data[i] = Op::template Apply<SrcT, DestT>(src_data[i]);
    });
  }
}

template <typename SrcT, typename Op>
void CastFromSrcType(const Vector &source, Vector *target, SqlTypeId target_type) {
  switch (target_type) {
    case SqlTypeId::Boolean:
      CastFromSrcTypeToDestType<SrcT, bool, Op>(source, target);
      break;
    case SqlTypeId::TinyInt:
      CastFromSrcTypeToDestType<SrcT, int8_t, Op>(source, target);
      break;
    case SqlTypeId::SmallInt:
      CastFromSrcTypeToDestType<SrcT, int16_t, Op>(source, target);
      break;
    case SqlTypeId::Integer:
      CastFromSrcTypeToDestType<SrcT, int32_t, Op>(source, target);
      break;
    case SqlTypeId::BigInt:
      CastFromSrcTypeToDestType<SrcT, int64_t, Op>(source, target);
      break;
    case SqlTypeId::Real:
      CastFromSrcTypeToDestType<SrcT, float, Op>(source, target);
      break;
    case SqlTypeId::Double:
      CastFromSrcTypeToDestType<SrcT, double, Op>(source, target);
      break;
    case SqlTypeId::Date:
      CastFromSrcTypeToDestType<SrcT, Date, tpl::sql::CastToDate>(source, target);
      break;
    case SqlTypeId::Varchar: {
      TPL_ASSERT(target->GetTypeId() == TypeId::Varchar, "Result vector must be string");
      auto src_data = reinterpret_cast<SrcT *>(source.GetData());
      auto result_data = reinterpret_cast<VarlenEntry *>(target->GetData());
      VectorOps::Exec(source, [&](uint64_t i, uint64_t k) {
        if (!source.GetNullMask()[i]) {
          auto str = Op::template Apply<SrcT, std::string>(src_data[i]);
          result_data[i] = target->GetMutableStringHeap()->AddVarlen(str);
        }
      });
      break;
    }
    default:
      throw NotImplementedException("casting vector of type '{}' to '{}' not supported",
                                    TypeIdToString(source.GetTypeId()),
                                    TypeIdToString(target->GetTypeId()));
  }
}

}  // namespace

void VectorOps::Cast(const Vector &source, Vector *target, SqlTypeId source_type,
                     SqlTypeId target_type) {
  target->Resize(source.GetSize());
  target->SetFilteredTupleIdList(source.GetFilteredTupleIdList(), source.GetCount());
  target->GetMutableNullMask()->Copy(source.GetNullMask());
  switch (source_type) {
    case SqlTypeId::Boolean:
      CastFromSrcType<bool, tpl::sql::Cast>(source, target, target_type);
      break;
    case SqlTypeId::TinyInt:
      CastFromSrcType<int8_t, tpl::sql::Cast>(source, target, target_type);
      break;
    case SqlTypeId::SmallInt:
      CastFromSrcType<int16_t, tpl::sql::Cast>(source, target, target_type);
      break;
    case SqlTypeId::Integer:
      CastFromSrcType<int32_t, tpl::sql::Cast>(source, target, target_type);
      break;
    case SqlTypeId::BigInt:
      CastFromSrcType<int64_t, tpl::sql::Cast>(source, target, target_type);
      break;
    case SqlTypeId::Real:
      CastFromSrcType<float, tpl::sql::Cast>(source, target, target_type);
      break;
    case SqlTypeId::Double:
      CastFromSrcType<double, tpl::sql::Cast>(source, target, target_type);
      break;
    case SqlTypeId::Date:
      CastFromSrcType<Date, tpl::sql::CastFromDate>(source, target, target_type);
      break;
    default:
      throw NotImplementedException("casting vector of type '{}' not supported",
                                    TypeIdToString(source.GetTypeId()));
  }
}

void VectorOps::Cast(const Vector &source, Vector *target) {
  const SqlTypeId src_type = GetSqlTypeFromInternalType(source.type_);
  const SqlTypeId target_type = GetSqlTypeFromInternalType(target->type_);
  Cast(source, target, src_type, target_type);
}

}  // namespace tpl::sql

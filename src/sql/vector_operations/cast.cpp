#include "sql/vector_operations/vector_operators.h"

#include <string>

#include "common/exception.h"
#include "sql/operations/cast_operators.h"
#include "sql/vector_operations/unary_op_helpers.h"

namespace tpl::sql {

// TODO(pmenon): String casts are identical. Refactor.
// TODO(pmenon): Beef up TemplatedUnaryOperation to support both templated
//               operations and functors?

namespace {

NotImplementedException UnimplementedCast(TypeId source_type, TypeId target_type) {
  return NotImplementedException("Unsupported cast: {} -> {}", TypeIdToString(source_type),
                                 TypeIdToString(target_type));
}

// Cast from a numeric-ish type into one of the many supported types.
template <typename InType>
void CastNumericOperation(const Vector &source, Vector *target, SqlTypeId target_type) {
  switch (target_type) {
    case SqlTypeId::Boolean:
      TemplatedUnaryOperation<InType, bool, tpl::sql::Cast>(source, target);
      break;
    case SqlTypeId::TinyInt:
      TemplatedUnaryOperation<InType, int8_t, tpl::sql::Cast>(source, target);
      break;
    case SqlTypeId::SmallInt:
      TemplatedUnaryOperation<InType, int16_t, tpl::sql::Cast>(source, target);
      break;
    case SqlTypeId::Integer:
      TemplatedUnaryOperation<InType, int32_t, tpl::sql::Cast>(source, target);
      break;
    case SqlTypeId::BigInt:
      TemplatedUnaryOperation<InType, int64_t, tpl::sql::Cast>(source, target);
      break;
    case SqlTypeId::Real:
      TemplatedUnaryOperation<InType, float, tpl::sql::Cast>(source, target);
      break;
    case SqlTypeId::Double:
      TemplatedUnaryOperation<InType, double, tpl::sql::Cast>(source, target);
      break;
    case SqlTypeId::Varchar: {
      TPL_ASSERT(target->GetTypeId() == TypeId::Varchar, "Result vector must be string");
      auto src_data = reinterpret_cast<InType *>(source.GetData());
      auto result_data = reinterpret_cast<VarlenEntry *>(target->GetData());
      VectorOps::Exec(source, [&](uint64_t i, uint64_t k) {
        if (!source.GetNullMask()[i]) {
          auto str = tpl::sql::Cast::template Apply<InType, std::string>(src_data[i]);
          result_data[i] = target->GetMutableStringHeap()->AddVarlen(str);
        }
      });
      break;
    }
    default:
      throw UnimplementedCast(source.GetTypeId(), target->GetTypeId());
  }
}

void CastDateOperation(const Vector &source, Vector *target, SqlTypeId target_type) {
  target->Resize(source.GetSize());
  target->GetMutableNullMask()->Copy(source.GetNullMask());
  target->SetFilteredTupleIdList(source.GetFilteredTupleIdList(), source.GetCount());
  
  switch (target_type) {
    case SqlTypeId::Varchar: {
      TPL_ASSERT(target->GetTypeId() == TypeId::Varchar, "Result vector must be string");
      auto src_data = reinterpret_cast<const Date *>(source.GetData());
      auto result_data = reinterpret_cast<VarlenEntry *>(target->GetData());
      VectorOps::Exec(source, [&](uint64_t i, uint64_t k) {
        if (!source.GetNullMask()[i]) {
          auto str = tpl::sql::CastFromDate::template Apply<Date, std::string>(src_data[i]);
          result_data[i] = target->GetMutableStringHeap()->AddVarlen(str);
        }
      });
      break;
    }
    default:
      throw UnimplementedCast(source.GetTypeId(), target->GetTypeId());
  }
}

void CastStringOperation(const Vector &source, Vector *target, SqlTypeId target_type) {
  throw UnimplementedCast(source.GetTypeId(), target->GetTypeId());
}

}  // namespace

void VectorOps::Cast(const Vector &source, Vector *target, SqlTypeId source_type,
                     SqlTypeId target_type) {
  switch (source_type) {
    case SqlTypeId::Boolean:
      CastNumericOperation<bool>(source, target, target_type);
      break;
    case SqlTypeId::TinyInt:
      CastNumericOperation<int8_t>(source, target, target_type);
      break;
    case SqlTypeId::SmallInt:
      CastNumericOperation<int16_t>(source, target, target_type);
      break;
    case SqlTypeId::Integer:
      CastNumericOperation<int32_t>(source, target, target_type);
      break;
    case SqlTypeId::BigInt:
      CastNumericOperation<int64_t>(source, target, target_type);
      break;
    case SqlTypeId::Real:
      CastNumericOperation<float>(source, target, target_type);
      break;
    case SqlTypeId::Double:
      CastNumericOperation<double>(source, target, target_type);
      break;
    case SqlTypeId::Date:
      CastDateOperation(source, target, target_type);
      break;
    case SqlTypeId::Char:
    case SqlTypeId::Varchar:
      CastStringOperation(source, target, target_type);
    default:
      throw UnimplementedCast(source.GetTypeId(), target->GetTypeId());
  }
}

void VectorOps::Cast(const Vector &source, Vector *target) {
  const SqlTypeId src_type = GetSqlTypeFromInternalType(source.type_);
  const SqlTypeId target_type = GetSqlTypeFromInternalType(target->type_);
  Cast(source, target, src_type, target_type);
}

}  // namespace tpl::sql

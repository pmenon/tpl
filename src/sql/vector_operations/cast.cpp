#include "sql/vector_operations/vector_operators.h"

#include "sql/operations/cast_operators.h"

namespace tpl::sql {

namespace {

template <typename SrcT, typename DestT, typename Op>
void CastFromSrcTypeToDestType(const Vector &source, Vector *target) {
  auto src_data = reinterpret_cast<SrcT *>(source.data());
  auto target_data = reinterpret_cast<DestT *>(target->data());
  if (!source.null_mask().any()) {
    // Fast path: no nulls in source vector, direct cast
    VectorOps::Exec(source, [&](u64 i, u64 k) {
      target_data[i] = Op::template Apply<SrcT, DestT>(src_data[i]);
    });
  } else {
    // Slow-path need to check NULLs
    VectorOps::Exec(source, [&](u64 i, u64 k) {
      if (!source.null_mask()[i]) {
        target_data[i] = Op::template Apply<SrcT, DestT>(src_data[i]);
      }
    });
  }
}

template <typename SrcT, typename Op>
void CastFromSrcType(const Vector &source, Vector *target,
                     SqlTypeId target_type) {
  switch (target_type) {
    case SqlTypeId::Boolean: {
      CastFromSrcTypeToDestType<SrcT, bool, Op>(source, target);
      break;
    }
    case SqlTypeId::TinyInt: {
      CastFromSrcTypeToDestType<SrcT, i8, Op>(source, target);
      break;
    }
    case SqlTypeId::SmallInt: {
      CastFromSrcTypeToDestType<SrcT, i16, Op>(source, target);
      break;
    }
    case SqlTypeId::Integer: {
      CastFromSrcTypeToDestType<SrcT, i32, Op>(source, target);
      break;
    }
    case SqlTypeId::BigInt: {
      CastFromSrcTypeToDestType<SrcT, i64, Op>(source, target);
      break;
    }
    case SqlTypeId::Real: {
      CastFromSrcTypeToDestType<SrcT, f32, Op>(source, target);
      break;
    }
    case SqlTypeId::Double: {
      CastFromSrcTypeToDestType<SrcT, f64, Op>(source, target);
      break;
    }
    case SqlTypeId::Decimal:
    case SqlTypeId::Date:
    case SqlTypeId::Char:
    case SqlTypeId::Varchar: {
      throw std::runtime_error("Implement me");
    }
  }
}

}  // namespace

void VectorOps::Cast(const Vector &source, Vector *target,
                     SqlTypeId source_type, SqlTypeId target_type) {
  target->sel_vector_ = source.sel_vector_;
  target->count_ = source.count_;
  target->null_mask_ = source.null_mask_;
  switch (source_type) {
    case SqlTypeId::Boolean: {
      CastFromSrcType<bool, tpl::sql::Cast>(source, target, target_type);
      break;
    }
    case SqlTypeId::TinyInt: {
      CastFromSrcType<i8, tpl::sql::Cast>(source, target, target_type);
      break;
    }
    case SqlTypeId::SmallInt: {
      CastFromSrcType<i16, tpl::sql::Cast>(source, target, target_type);
      break;
    }
    case SqlTypeId::Integer: {
      CastFromSrcType<i32, tpl::sql::Cast>(source, target, target_type);
      break;
    }
    case SqlTypeId::BigInt: {
      CastFromSrcType<i64, tpl::sql::Cast>(source, target, target_type);
      break;
    }
    case SqlTypeId::Real: {
      CastFromSrcType<f32, tpl::sql::Cast>(source, target, target_type);
      break;
    }
    case SqlTypeId::Double: {
      CastFromSrcType<f64, tpl::sql::Cast>(source, target, target_type);
      break;
    }
    case SqlTypeId::Decimal:
    case SqlTypeId::Date:
    case SqlTypeId::Char:
    case SqlTypeId::Varchar: {
      throw std::runtime_error("Implement me");
    }
  }
}

void VectorOps::Cast(const Vector &source, Vector *target) {
  const auto source_sql_type = GetSqlTypeFromInternalType(source.type_);
  const auto target_sql_type = GetSqlTypeFromInternalType(target->type_);
  Cast(source, target, source_sql_type, target_sql_type);
}

}  // namespace tpl::sql
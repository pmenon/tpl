#pragma once

#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "tpl_test.h"  // NOLINT

#include "sql/catalog.h"
#include "sql/vector.h"

namespace tpl {

class SqlBasedTest : public TplTest {
 protected:
  void SetUp() override {
    TplTest::SetUp();
    sql::Catalog::Instance();
  }

  static u16 TableIdToNum(sql::TableId table_id) { return static_cast<u16>(table_id); }
};

#define MAKE_VEC_TYPE(TYPE, CPP_TYPE)                                                              \
  static inline std::unique_ptr<sql::Vector> Make##TYPE##Vector(u32 size) {                        \
    return std::make_unique<sql::Vector>(sql::TypeId::TYPE, size, true);                           \
  }                                                                                                \
  static inline std::unique_ptr<sql::Vector> Make##TYPE##Vector() {                                \
    return Make##TYPE##Vector(kDefaultVectorSize);                                                 \
  }                                                                                                \
  static inline std::unique_ptr<sql::Vector> Make##TYPE##Vector(const std::vector<CPP_TYPE> &vals, \
                                                                const std::vector<bool> &nulls) {  \
    auto vec = Make##TYPE##Vector(vals.size());                                                    \
    for (u64 i = 0; i < vals.size(); i++) {                                                        \
      if (nulls[i]) {                                                                              \
        vec->SetValue(i, sql::GenericValue::CreateNull(vec->type_id()));                           \
      } else {                                                                                     \
        vec->SetValue(i, sql::GenericValue::Create##TYPE(vals[i]));                                \
      }                                                                                            \
    }                                                                                              \
    return vec;                                                                                    \
  }

MAKE_VEC_TYPE(Boolean, bool)
MAKE_VEC_TYPE(TinyInt, i8)
MAKE_VEC_TYPE(SmallInt, i16)
MAKE_VEC_TYPE(Integer, i32)
MAKE_VEC_TYPE(BigInt, i64)
MAKE_VEC_TYPE(Float, f32)
MAKE_VEC_TYPE(Double, f64)
MAKE_VEC_TYPE(Varchar, std::string_view)

#undef MAKE_VEC_TYPE
#undef MAKE_VEC_TYPE_IMPL

}  // namespace tpl

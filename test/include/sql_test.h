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

  static uint16_t TableIdToNum(sql::TableId table_id) { return static_cast<uint16_t>(table_id); }
};

#define MAKE_VEC_TYPE(TYPE, CPP_TYPE)                                                              \
  static inline std::unique_ptr<sql::Vector> Make##TYPE##Vector(uint32_t size) {                        \
    return std::make_unique<sql::Vector>(sql::TypeId::TYPE, size, true);                           \
  }                                                                                                \
  static inline std::unique_ptr<sql::Vector> Make##TYPE##Vector() {                                \
    return Make##TYPE##Vector(kDefaultVectorSize);                                                 \
  }                                                                                                \
  static inline std::unique_ptr<sql::Vector> Make##TYPE##Vector(const std::vector<CPP_TYPE> &vals, \
                                                                const std::vector<bool> &nulls) {  \
    auto vec = Make##TYPE##Vector(vals.size());                                                    \
    for (uint64_t i = 0; i < vals.size(); i++) {                                                        \
      if (nulls[i]) {                                                                              \
        vec->SetValue(i, sql::GenericValue::CreateNull(vec->type_id()));                           \
      } else {                                                                                     \
        vec->SetValue(i, sql::GenericValue::Create##TYPE(vals[i]));                                \
      }                                                                                            \
    }                                                                                              \
    return vec;                                                                                    \
  }

MAKE_VEC_TYPE(Boolean, bool)
MAKE_VEC_TYPE(TinyInt, int8_t)
MAKE_VEC_TYPE(SmallInt, int16_t)
MAKE_VEC_TYPE(Integer, int32_t)
MAKE_VEC_TYPE(BigInt, int64_t)
MAKE_VEC_TYPE(Float, float)
MAKE_VEC_TYPE(Double, double)
MAKE_VEC_TYPE(Varchar, std::string_view)

#undef MAKE_VEC_TYPE
#undef MAKE_VEC_TYPE_IMPL

}  // namespace tpl

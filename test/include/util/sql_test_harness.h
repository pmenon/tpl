#pragma once

#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "sql/catalog.h"
#include "sql/vector.h"
#include "util/test_harness.h"

namespace tpl {

class SqlBasedTest : public TplTest {
 protected:
  void SetUp() override {
    TplTest::SetUp();
    sql::Catalog::Instance();
  }

  static uint16_t TableIdToNum(sql::TableId table_id) { return static_cast<uint16_t>(table_id); }
};

static inline std::unique_ptr<sql::Vector> MakeVector(sql::TypeId type_id, uint32_t size) {
  auto vec = std::make_unique<sql::Vector>(type_id, true, true);
  vec->Resize(size);
  return vec;
}

#define MAKE_VEC_TYPE(TYPE, CPP_TYPE)                                                              \
  static inline std::unique_ptr<sql::Vector> Make##TYPE##Vector(uint32_t size) {                   \
    return MakeVector(sql::TypeId::TYPE, size);                                                    \
  }                                                                                                \
  static inline std::unique_ptr<sql::Vector> Make##TYPE##Vector(const std::vector<CPP_TYPE> &vals, \
                                                                const std::vector<bool> &nulls) {  \
    TPL_ASSERT(vals.size() == nulls.size(), "Value and NULL vector sizes don't match");            \
    auto vec = Make##TYPE##Vector(vals.size());                                                    \
    for (uint64_t i = 0; i < vals.size(); i++) {                                                   \
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

#include "common/macros.h"
#include "record/types.h"
#include "record/field.h"

inline int CompareStrings(const char *str1, int len1, const char *str2, int len2) {
  assert(str1 != nullptr);
  assert(len1 >= 0);
  assert(str2 != nullptr);
  assert(len2 >= 0);
  int ret = memcmp(str1, str2, static_cast<size_t>(std::min(len1, len2)));
  if (ret == 0 && len1 != len2) {
    ret = len1 - len2;
  }
  return ret;
}

// ==============================Type=============================

Type *Type::type_singletons_[] = {
        new Type(TypeId::kTypeInvalid),
        new TypeInt(),
        new TypeFloat(),
        new TypeChar()
};

uint32_t Type::SerializeTo(const Field &field, char *buf) const {
  return SerializeTo(field,buf);
}

uint32_t Type::DeserializeFrom(char *storage, Field **field, bool is_null, MemHeap *heap) const {
  return DeserializeFrom(storage,field,is_null,heap);
}

uint32_t Type::GetSerializedSize(const Field &field, bool is_null) const {
  if(is_null) return 0;
  return field.GetSerializedSize();
}

const char *Type::GetData(const Field &val) const {
  std::string str("");
  switch(val.type_id_){
    case TypeId::kTypeInt : str+=val.value_.integer_;break;
    case TypeId::kTypeFloat: str+=val.value_.float_;break;
    case TypeId::kTypeChar: str += val.value_.chars_;break;
    default: assert(false);
  }
  char *res = new char[str.length()+1];
  memcpy(res,(char*)&str[0],str.length()+1);
  return res;
}

uint32_t Type::GetLength(const Field &val) const {
  return val.GetLength();
}

CmpBool Type::CompareEquals(const Field &left, const Field &right) const {
    return CompareEquals(left,right);
}

CmpBool Type::CompareNotEquals(const Field &left, const Field &right) const {
  return CompareNotEquals(left,right);
}

CmpBool Type::CompareLessThan(const Field &left, const Field &right) const {
  return CompareLessThan(left,right);
}

CmpBool Type::CompareLessThanEquals(const Field &left, const Field &right) const {
  return CompareLessThanEquals(left,right);
}

CmpBool Type::CompareGreaterThan(const Field &left, const Field &right) const {
  return CompareGreaterThan(left,right);
}

CmpBool Type::CompareGreaterThanEquals(const Field &left, const Field &right) const {
  return CompareGreaterThanEquals(left,right);
}

// ==============================TypeInt=================================

uint32_t TypeInt::SerializeTo(const Field &field, char *buf) const {
  if (!field.IsNull()) {
    MACH_WRITE_TO(int32_t, buf, field.value_.integer_);
    return GetTypeSize(type_id_);
  }
  return 0;
}

uint32_t TypeInt::DeserializeFrom(char *storage, Field **field, bool is_null, MemHeap *heap) const {
  if (is_null) {
    *field = ALLOC_P(heap, Field)(TypeId::kTypeInt);
    return 0;
  }
  int32_t val = MACH_READ_FROM(int32_t, storage);
  *field = ALLOC_P(heap, Field)(TypeId::kTypeInt, val);
  return GetTypeSize(type_id_);
}

uint32_t TypeInt::GetSerializedSize(const Field &field, bool is_null) const {
  if (is_null) {
    return 0;
  }
  return GetTypeSize(type_id_);
}

CmpBool TypeInt::CompareEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.integer_ == right.value_.integer_);
}

CmpBool TypeInt::CompareNotEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.integer_ != right.value_.integer_);
}

CmpBool TypeInt::CompareLessThan(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.integer_ < right.value_.integer_);
}

CmpBool TypeInt::CompareLessThanEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.integer_ <= right.value_.integer_);
}

CmpBool TypeInt::CompareGreaterThan(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.integer_ > right.value_.integer_);
}

CmpBool TypeInt::CompareGreaterThanEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.integer_ >= right.value_.integer_);
}

// ==============================TypeFloat=============================

uint32_t TypeFloat::SerializeTo(const Field &field, char *buf) const {
  if (!field.IsNull()) {
    MACH_WRITE_TO(float_t, buf, field.value_.float_);
    return GetTypeSize(type_id_);
  }
  return 0;
}

uint32_t TypeFloat::DeserializeFrom(char *storage, Field **field, bool is_null, MemHeap *heap) const {
  if (is_null) {
    *field = ALLOC_P(heap, Field)(TypeId::kTypeFloat);
    return 0;
  }
  float_t val = MACH_READ_FROM(float_t, storage);
  *field = ALLOC_P(heap, Field)(TypeId::kTypeFloat, val);
  return GetTypeSize(type_id_);
}

uint32_t TypeFloat::GetSerializedSize(const Field &field, bool is_null) const {
  if (is_null) {
    return 0;
  }
  return GetTypeSize(type_id_);
}

CmpBool TypeFloat::CompareEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.float_ == right.value_.float_);
}

CmpBool TypeFloat::CompareNotEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.float_ != right.value_.float_);
}

CmpBool TypeFloat::CompareLessThan(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.float_ < right.value_.float_);
}

CmpBool TypeFloat::CompareLessThanEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.float_ <= right.value_.float_);
}

CmpBool TypeFloat::CompareGreaterThan(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.float_ > right.value_.float_);
}

CmpBool TypeFloat::CompareGreaterThanEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(left.value_.float_ >= right.value_.float_);
}

// ==============================TypeChar=============================
uint32_t TypeChar::SerializeTo(const Field &field, char *buf) const {
  if (!field.IsNull()) {
    uint32_t len = GetLength(field);
    memcpy(buf, &len, sizeof(uint32_t));
    memcpy(buf + sizeof(uint32_t), field.value_.chars_, len);
    return len + sizeof(uint32_t);
  }
  return 0;
}

uint32_t TypeChar::DeserializeFrom(char *storage, Field **field, bool is_null, MemHeap *heap) const {
  if (is_null) {
    *field = ALLOC_P(heap, Field)(TypeId::kTypeChar);
    return 0;
  }
  uint32_t len = MACH_READ_UINT32(storage);
  *field = ALLOC_P(heap, Field)(TypeId::kTypeChar, storage + sizeof(uint32_t), len, true);
  return len + sizeof(uint32_t);
}

uint32_t TypeChar::GetSerializedSize(const Field &field, bool is_null) const {
  if (is_null) {
    return 0;
  }
  uint32_t len = GetLength(field);
  return len + sizeof(uint32_t);
}

const char *TypeChar::GetData(const Field &val) const {
  return val.value_.chars_;
}

uint32_t TypeChar::GetLength(const Field &val) const {
  return val.len_;
}

CmpBool TypeChar::CompareEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(CompareStrings(left.GetData(), left.GetLength(), right.GetData(), right.GetLength()) == 0);
}

CmpBool TypeChar::CompareNotEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(CompareStrings(left.GetData(), left.GetLength(), right.GetData(), right.GetLength()) != 0);
}

CmpBool TypeChar::CompareLessThan(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(CompareStrings(left.GetData(), left.GetLength(), right.GetData(), right.GetLength()) < 0);
}

CmpBool TypeChar::CompareLessThanEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(CompareStrings(left.GetData(), left.GetLength(), right.GetData(), right.GetLength()) <= 0);
}

CmpBool TypeChar::CompareGreaterThan(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(CompareStrings(left.GetData(), left.GetLength(), right.GetData(), right.GetLength()) > 0);
}

CmpBool TypeChar::CompareGreaterThanEquals(const Field &left, const Field &right) const {
  ASSERT(left.CheckComparable(right), "Not comparable.");
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::kNull;
  }
  return GetCmpBool(CompareStrings(left.GetData(), left.GetLength(), right.GetData(), right.GetLength()) >= 0);
}
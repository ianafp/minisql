#include "record/column.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  // write magic num
  uint16_t offset = 0;
  MACH_WRITE_TO(uint32_t, buf + offset, COLUMN_MAGIC_NUM);
  offset += sizeof(COLUMN_MAGIC_NUM);
  // write name : string
  // write length of string
  uint32_t len = name_.length()+1;
  MACH_WRITE_TO(uint32_t, buf + offset, len);
  offset += sizeof(len);
  // addr
  char* addr = (char*)&name_[0];
  memcpy(buf+offset,addr,len);
  offset += len;
  // write type : TypeId
  MACH_WRITE_TO(TypeId, buf+offset, type_);
  offset += sizeof(TypeId);
  // write length
  MACH_WRITE_TO(uint32_t, buf+offset, len_);
  offset += sizeof(uint32_t);
  // write table index
  MACH_WRITE_TO(uint32_t, buf+offset, table_ind_);
  offset += sizeof(uint32_t);
  // write is_null_enable
  MACH_WRITE_TO(bool, buf+offset, nullable_);
  offset += sizeof(bool);
  // write is_uniqe
  MACH_WRITE_TO(bool, buf+offset, unique_);
  offset += sizeof(bool);
  return offset;
}

uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  return sizeof(uint32_t) * 4 + sizeof(bool) * 2 + sizeof(TypeId) + 1 + name_.length();

}

uint32_t Column::DeserializeFrom(char *buf, Column* &column, MemHeap *heap) {
  // replace with your code here
  // read magic num
  uint16_t offset = 0;
  uint32_t r_magic_num = MACH_READ_FROM(uint32_t,buf+offset);
  if(r_magic_num!=Column::COLUMN_MAGIC_NUM){
#ifdef ENABLE_BPM_DEBUG
    log(ERROR)<<"The magic number not match when deserialize column!\n";
#endif
    return 0;
  }
  // magic number matched
  offset += sizeof(Column::COLUMN_MAGIC_NUM);
  // read name
  // read name length
  uint32_t name_len = MACH_READ_FROM(uint32_t,buf+offset);
  offset += sizeof(uint32_t);
  // read string
  std::string temp_str(buf + offset);
  offset += name_len;
  // read type
  TypeId temp_type = MACH_READ_FROM(TypeId,buf+offset);
  offset += sizeof(TypeId);
  // read length
  uint32_t temp_len = MACH_READ_FROM(uint32_t,buf+offset);
  offset += sizeof(uint32_t);
  // read table index 
  uint32_t temp_index = MACH_READ_FROM(uint32_t,buf+offset);
  offset += sizeof(uint32_t);
  // read null enable
  bool temp_nulleble = MACH_READ_FROM(bool,buf+offset);
  offset += sizeof(bool);
  // read unique
  bool temp_uniqe = MACH_READ_FROM(bool,buf+offset);
  offset += sizeof(bool);
  // consrtcut column
  if(temp_type==TypeId::kTypeChar)
  column = ALLOC_P(heap,Column)(temp_str,temp_type,temp_len,temp_index,temp_nulleble,temp_uniqe);
  else 
    column = ALLOC_P(heap,Column)(temp_str,temp_type,temp_index,temp_nulleble,temp_uniqe);
  return offset;
}

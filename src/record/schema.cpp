#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t offset = 0;
  // write magic number
  MACH_WRITE_TO(uint32_t,buf+offset,Schema::SCHEMA_MAGIC_NUM);
  offset += sizeof(Schema::SCHEMA_MAGIC_NUM);
  // write columns count
  MACH_WRITE_TO(uint32_t,buf+offset,columns_.size());
  offset += sizeof(uint32_t);
  // write columns
  for(auto i:columns_){
    offset += i->SerializeTo(buf+offset);
  }
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t offset = sizeof(Schema::SCHEMA_MAGIC_NUM) + sizeof(uint32_t);
  for(auto i:columns_){
    offset += i->GetSerializedSize();
  }
  return offset;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // replace with your code here
  // read magic num
  uint32_t offset = 0;
  uint32_t count = MACH_READ_UINT32(buf+offset);
  assert(count==Schema::SCHEMA_MAGIC_NUM);

  // create colums
  offset += sizeof(Schema::SCHEMA_MAGIC_NUM);
  std::vector<Column*> cols;
  // read count
  count = MACH_READ_UINT32(buf+offset);
  offset += sizeof(Schema::SCHEMA_MAGIC_NUM);
  cols.resize(count);
  for(uint32_t i=0;i<count;++i){
    offset += Column::DeserializeFrom(buf+offset,cols[i],heap);
  }
  // allocate schema
  schema = ALLOC_P(heap,Schema)(cols);
  return offset;
}
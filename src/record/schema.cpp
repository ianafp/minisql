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
  uint32_t count = MACH_READ_FROM(uint32_t,buf+offset);
  if(count!=Schema::SCHEMA_MAGIC_NUM){
#ifdef ENABLE_BPM_DEBUG
  LOG(ERROR)<<"SCHEMA MAGIN NUM NOT MATCHED!\n";
#endif
    return 0;
  }
  offset += sizeof(Schema::SCHEMA_MAGIC_NUM);
  // read count
  count = MACH_READ_FROM(uint32_t,buf+offset);
  offset += sizeof(Schema::SCHEMA_MAGIC_NUM);
  // read columns
  if(count>schema->columns_.capacity()){
    schema->columns_.resize(count+10);
  }
  for(uint32_t i=0;i<count;++i){
    offset += Column::DeserializeFrom(buf+offset,schema->columns_[i],heap);
  }
  return offset;
}
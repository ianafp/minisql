#include "record/row.h"
#include "glog/logging.h"
#define ENABLE_BPM_DEBUG
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
  // assert(false);
  uint32_t offset = 0;
  // write row id

  // buf[0] = 16;
  
  MACH_WRITE_TO(RowId, buf + offset, rid_);
  // assert(false);

  offset += sizeof(RowId);
  // write fields
  uint32_t column_count = schema->GetColumnCount();
  uint32_t i = 0;
  // assert(false);
  for(i=0;i<column_count;i++)
  {
    MACH_WRITE_TO(bool,buf+offset,fields_[i]->IsNull());
    offset += sizeof(bool);
    offset += fields_[i]->SerializeTo(buf+offset);
  }

  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  // replace with your code here
  // read row id
  uint32_t offset = 0;
  // read id
  this->rid_ = MACH_READ_FROM(RowId, buf + offset);
  offset += sizeof(RowId);
  // get column count
  uint32_t column_count = schema->GetColumnCount();
  if(column_count>fields_.capacity()) fields_.resize(column_count);
  for (uint32_t i = 0; i < column_count; ++i) {
    bool is_null = MACH_READ_FROM(bool,buf+offset);
    offset += sizeof(bool);
    if(is_null) continue;
    const Column *temp = schema->GetColumn(i);
    offset += this->fields_[i]->DeserializeFrom(buf + offset, temp->GetType(), &fields_[i],is_null,
    heap_);
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  // replace with your code here
  uint32_t offset = 0;
  uint32_t colomn_count = schema->GetColumnCount();
  for (uint32_t i=0;i<colomn_count;++i) {
    offset += sizeof(bool);
    offset += this->fields_[i]->GetSerializedSize();
  }
  // if(offset==0) return 0;
  offset += sizeof(RowId);
  return offset;
}

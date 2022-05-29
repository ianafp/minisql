#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  MACH_WRITE_TO(uint32_t,buf+offset,IndexMetadata::INDEX_METADATA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(uint32_t,buf+offset,index_id_);
  offset += sizeof(uint32_t);
  uint32_t len = index_name_.length() + 1;
  char* addr = (char *)(&index_name_[0]);
  MACH_WRITE_TO(uint32_t,buf+offset,len);
  offset += sizeof(uint32_t);
  memcpy(buf+offset,addr,len);
  offset += len;
  MACH_WRITE_TO(uint32_t,buf+offset,table_id_);
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(uint32_t,buf+offset,key_map_.size());
  offset += sizeof(uint32_t);
  for(auto it : key_map_){
    MACH_WRITE_TO(uint32_t,buf+offset,it);
    offset += sizeof(uint32_t);
  }
  return offset;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  return sizeof(uint32_t)*(6+key_map_.size()+index_name_.length());
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  uint32_t offset = 0;
  assert(IndexMetadata::INDEX_METADATA_MAGIC_NUM==MACH_READ_UINT32(buf+offset));
  offset += sizeof(uint32_t);
  index_meta = ALLOC_P(heap,IndexMetadata)();
  index_meta->index_id_ = MACH_READ_UINT32(buf+offset);
  offset += sizeof(uint32_t);
  uint32_t len = MACH_READ_UINT32(buf+offset);
  offset += sizeof(uint32_t);
  index_meta->index_name_ = std::string(buf+offset);
  offset += len;
  index_meta->table_id_ = MACH_READ_UINT32(buf+offset);
  offset += sizeof(uint32_t);
  uint32_t size = MACH_READ_UINT32(buf+offset);
  offset += sizeof(uint32_t);
  for(uint32_t i=0;i<size;++i){
    index_meta->key_map_.push_back(MACH_READ_UINT32(buf+offset));
    offset += sizeof(uint32_t);
  }
  return offset;
}
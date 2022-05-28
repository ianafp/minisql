#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  MACH_WRITE_TO(uint32_t,buf+offset,TableMetadata::TABLE_METADATA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(uint32_t,buf+offset,table_id_);
  offset += sizeof(uint32_t);
  uint32_t len = table_name_.length() + 1;
  char* addr = (char *)(&table_name_[0]);
  MACH_WRITE_TO(uint32_t,buf+offset,len);
  offset += sizeof(uint32_t);
  memcpy(buf+offset,addr,len);
  offset += len;
  MACH_WRITE_TO(uint32_t,buf+offset,root_page_id_);
  offset += sizeof(uint32_t);
  offset+=schema_->SerializeTo(buf);
  return offset;
}

uint32_t TableMetadata::GetSerializedSize() const {
  return sizeof(uint32_t)*(5+table_name_.length())+schema_->GetSerializedSize();
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  uint32_t offset = 0;
  assert(TableMetadata::TABLE_METADATA_MAGIC_NUM==MACH_READ_FROM(uint32_t,buf+offset));
  offset += sizeof(uint32_t);

  table_meta = ALLOC_P(heap,TableMetadata)();
  table_meta ->table_id_ = MACH_READ_FROM(uint32_t,buf+offset);
  offset += sizeof(uint32_t);

  uint32_t len = MACH_READ_FROM(uint32_t,buf+offset);
  offset += sizeof(uint32_t);

  table_meta->table_name_ = std::string(buf+offset);
  offset += len;

  table_meta -> root_page_id_ = MACH_READ_FROM(uint32_t,buf+offset);

  offset += Schema::DeserializeFrom(buf+offset,table_meta->schema_,heap);
  return offset;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}

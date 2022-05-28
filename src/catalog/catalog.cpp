#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  // ASSERT(false, "Not Implemented yet");
  uint32_t offset = 0;
  MACH_WRITE_TO(uint32_t,buf+offset,CatalogMeta::CATALOG_METADATA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(uint32_t,buf+offset,table_meta_pages_.size());
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(uint32_t,buf+offset,index_meta_pages_.size());
  offset += sizeof(uint32_t);
  // serialize map
  for(auto &it :table_meta_pages_){
    MACH_WRITE_TO(uint32_t,buf+offset,it.first);
    offset += sizeof(uint32_t);
    MACH_WRITE_TO(int32_t,buf+offset,it.second);
    offset += sizeof(int32_t);
  }
  for(auto &it: index_meta_pages_){
    MACH_WRITE_TO(uint32_t,buf+offset,it.first);
    offset += sizeof(uint32_t);
    MACH_WRITE_TO(int32_t,buf+offset,it.second);
    offset += sizeof(int32_t);
  }
  
  
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  // ASSERT(false, "Not Implemented yet");
  uint32_t offset = 0;
  assert(CatalogMeta::CATALOG_METADATA_MAGIC_NUM==MACH_READ_FROM(uint32_t,buf+offset));
  offset += sizeof(uint32_t);
  uint32_t table_meta_page_count = MACH_READ_FROM(uint32_t,buf+offset);
  offset += sizeof(uint32_t);
  uint32_t index_meta_page_count = MACH_READ_FROM(uint32_t,buf+offset);
  offset += sizeof(uint32_t);
  // create meta object
  CatalogMeta* res = ALLOC_P(heap,CatalogMeta)();
  for( int i=0;i<table_meta_page_count;++i){
    table_id_t par1 = MACH_READ_FROM(uint32_t,buf+offset);
    offset += sizeof(uint32_t);
    page_id_t par2 = MACH_READ_FROM(int32_t,buf+offset);
    offset += sizeof(int32_t);
    res->table_meta_pages_.insert(std::pair<table_id_t,page_id_t>(par1,par2));
  }
  for( int i=0;i<index_meta_page_count;++i){
    table_id_t par1 = MACH_READ_FROM(uint32_t,buf+offset);
    offset += sizeof(uint32_t);
    page_id_t par2 = MACH_READ_FROM(int32_t,buf+offset);
    offset += sizeof(int32_t);
    res->index_meta_pages_.insert(std::pair<table_id_t,page_id_t>(par1,par2));
  }
  return res;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  return sizeof(uint32_t)*3+(table_meta_pages_.size()+index_meta_pages_.size())*(sizeof(uint32_t)+sizeof(int32_t));
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  // ASSERT(false, "Not Implemented yet");
  if(init){
    // initialize the meta data
    int catalog_page_id_;
    this->buffer_pool_manager_->NewPage(catalog_page_id_);
    this->buffer_pool_manager_->UnpinPage(catalog_page_id_);
    this->catalog_meta_ = CatalogMeta::NewInstance(this->heap_);
    return;
  }
  char* buf = this->buffer_pool_manager_->FetchPage(0)->GetData();
  this->catalog_meta_ = CatalogMeta::DeserializeFrom(buf,this->heap_);
  

}

CatalogManager::~CatalogManager() {
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}
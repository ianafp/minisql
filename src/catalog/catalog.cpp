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
  assert(CatalogMeta::CATALOG_METADATA_MAGIC_NUM==MACH_READ_UINT32(buf+offset));
  offset += sizeof(uint32_t);
  uint32_t table_meta_page_count = MACH_READ_UINT32(buf+offset);
  offset += sizeof(uint32_t);
  uint32_t index_meta_page_count = MACH_READ_UINT32(buf+offset);
  offset += sizeof(uint32_t);
  // create meta object
  CatalogMeta* res = ALLOC_P(heap,CatalogMeta)();
  for( uint32_t i=0;i<table_meta_page_count;++i){
    table_id_t par1 = MACH_READ_UINT32(buf+offset);
    offset += sizeof(uint32_t);
    page_id_t par2 = MACH_READ_INT32(buf+offset);
    offset += sizeof(int32_t);
    res->table_meta_pages_.insert(std::pair<table_id_t,page_id_t>(par1,par2));
  }
  for( uint32_t i=0;i<index_meta_page_count;++i){
    table_id_t par1 =MACH_READ_UINT32(buf+offset);
    offset += sizeof(uint32_t);
    page_id_t par2 = MACH_READ_INT32(buf+offset);
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
    // int catalog_page_id_;
    // this->buffer_pool_manager_->NewPage(catalog_page_id_);
    // int table_heap_page_id_;
    // this->buffer_pool_manager_->NewPage(table_heap_page_id_);
    // this->buffer_pool_manager_->UnpinPage(table_heap_page_id_,false);
    // this->buffer_pool_manager_->UnpinPage(catalog_page_id_,false);
    this->catalog_meta_ = CatalogMeta::NewInstance(this->heap_);
    this->next_table_id_ = 0;
    this->next_index_id_ = 0;
    return;
  }
  // fetch catalog meta data
  char* buf = this->buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData();
  this->catalog_meta_ = CatalogMeta::DeserializeFrom(buf,this->heap_);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,false);
  // need not to fetch index root page
  

  // create table info
  // fetch map from catelog manager

  for(auto &it:this->catalog_meta_->table_meta_pages_){
    buf = this->buffer_pool_manager_->FetchPage(it.second)->GetData();
    this->buffer_pool_manager_->UnpinPage(it.second,false);
    TableMetadata* table_meta ;TableMetadata::DeserializeFrom(buf,table_meta,this->heap_);
    if(it.first>=next_table_id_) next_index_id_  = it.first+1;
    table_names_.insert(std::pair<std::string,table_id_t>(table_meta->GetTableName(),it.first));
    TableInfo* table_info = TableInfo::Create(this->heap_);
    TableHeap* table_heap = TableHeap::Create(this->buffer_pool_manager_,table_meta->GetFirstPageId(),table_meta->GetSchema(),this->log_manager_,this->lock_manager_,this->heap_);
    table_info->Init(table_meta,table_heap);
    this->tables_.insert(std::pair<table_id_t,TableInfo*>(it.first,table_info));
    this->index_names_.insert(std::pair<std::string, std::unordered_map<std::string, index_id_t>>(table_meta->GetTableName(),std::unordered_map<std::string,index_id_t>()));
  }

  for(auto &it:this->catalog_meta_->index_meta_pages_){
    buf = this->buffer_pool_manager_->FetchPage(it.second)->GetData();
    this->buffer_pool_manager_->UnpinPage(it.second,false);
    IndexMetadata* index_meta;
    IndexMetadata::DeserializeFrom(buf,index_meta,this->heap_);
    if(next_index_id_<=it.first) next_index_id_ = it.first + 1;

    table_id_t table_id = index_meta->GetTableId();
    TableInfo* table_info = tables_.find(table_id)->second;
    IndexInfo* index_info = IndexInfo::Create(this->heap_);
    std::string table_name = table_info->GetTableName();
    auto& table_index_map = this->index_names_.find(table_name)->second;
    table_index_map.insert(std::pair<std::string,index_id_t>(index_meta->GetIndexName(),it.first));
    index_info->Init(index_meta,table_info,this->buffer_pool_manager_);
    this->indexes_.insert(std::pair<index_id_t,IndexInfo*>(it.first,index_info));
  }


}

CatalogManager::~CatalogManager() {
  this->catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID);
  // std::vector<TableInfo*> table_info_;
  // std::vector<TableInfo*> index_info_;
  // this->GetTables(table_info_);
  // this->GetIndex(index_info_);
  
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  table_id_t table_id = next_index_id_ ++;
  if(next_index_id_==0) return DB_FAILED;
  auto it = this->table_names_.find(table_name);
  if(it!=this->table_names_.end()) return DB_TABLE_ALREADY_EXIST;
  page_id_t root_page;
  page_id_t meta_page_id_;
  
  // allocate page for the table meta page and table first page 
  this->buffer_pool_manager_->NewPage(meta_page_id_)->GetData();
  this->buffer_pool_manager_->UnpinPage(meta_page_id_);
  this->buffer_pool_manager_->NewPage(root_page);
  this->buffer_pool_manager_->UnpinPage(root_page);
  this->table_names_.insert(std::pair<std::string,table_id_t>(table_name,table_id));
  TableMetadata* table_meta = TableMetadata::Create(table_id,table_name,root_page,schema,this->heap_);
  char* buf = this->buffer_pool_manager_->FetchPage(meta_page_id_)->GetData();
  table_meta->SerializeTo(buf);
  buffer_pool_manager_->UnpinPage(meta_page_id_);
  table_info = TableInfo::Create(this->heap_);
  TableHeap* table_heap = TableHeap::Create(this->buffer_pool_manager_,table_meta->GetFirstPageId(),table_meta->GetSchema(),this->log_manager_,this->lock_manager_,this->heap_);
  
  table_info->Init(table_meta,table_heap);
  this->index_names_.insert(std::pair<std::string,std::unordered_map<std::string,index_id_t>>(table_name,std::unordered_map<std::string,index_id_t>()));
  this->tables_.insert(std::pair<table_id_t,TableInfo*>(table_id,table_info));

  this->catalog_meta_->table_meta_pages_.insert(std::pair<table_id_t,page_id_t>(table_id,meta_page_id_));
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto it = this->table_names_.find(table_name);
  if(it == this->table_names_.end())return DB_TABLE_NOT_EXIST;
  table_info = this->tables_.find(it->second)->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for(auto &it:this->tables_){
    tables.push_back(it.second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  auto it = this->table_names_.find(table_name);
  if(it==this->table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto it_index = this->index_names_.find(table_name)->second;
  auto it_index_name_id = it_index.find(index_name);
  if(it_index_name_id != it_index.end())
  {
    return DB_INDEX_ALREADY_EXIST;
  }
  // find the index name map for the table
  index_id_t index_id = next_index_id_++;
  if(next_index_id_==0) return DB_FAILED;
  it_index.insert(std::pair<std::string,index_id_t>(index_name,index_id));
  index_info = IndexInfo::Create(this->heap_);
  // fetch table info
  auto it_table = this->table_names_.find(table_name);
  if(it_table==this->table_names_.end()) return DB_FAILED;
  table_id_t table_id = it_table->second;
  TableInfo* table_info = this->tables_.find(table_id)->second;
  Schema* schema = table_info->GetSchema();
  vector<uint32_t> key_map;
  for(auto t:index_keys){
    uint32_t key_index;
    if(schema->GetColumnIndex(t,key_index)==DB_COLUMN_NAME_NOT_EXIST) return DB_COLUMN_NAME_NOT_EXIST;
    key_map.push_back(key_index);
  }
  IndexMetadata* index_meta =IndexMetadata::Create(index_id,index_name,table_id,key_map,this->heap_);
  
  index_info = IndexInfo::Create(this->heap_);
  index_info->Init(index_meta,table_info,this->buffer_pool_manager_);

  // persist
  page_id_t index_meta_page_id;
  // new page for the meta page and the index root page
  char* buf = this->buffer_pool_manager_->NewPage(index_meta_page_id)->GetData();
  buffer_pool_manager_->UnpinPage(index_meta_page_id);
  index_meta->SerializeTo(buf);
  this->catalog_meta_->index_meta_pages_.insert(std::pair<index_id_t,page_id_t>(index_id,index_meta_page_id));
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {

  auto it = this->index_names_.find(table_name);
  if(it==this->index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  auto it_index = it->second.find(index_name);
  if(it_index==it->second.end()){
    return DB_TABLE_NOT_EXIST;
  }
  index_info = this->indexes_.find(it_index->second) -> second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  for(auto &it:this->indexes_){
    indexes.push_back(it.second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  auto it = this->table_names_.find(table_name);
  if(it == this->table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = it->second;
  this->table_names_.erase(table_name);
  this->tables_.erase(table_id);
  vector<index_id_t> index_id;
  auto &it_index = this->index_names_.find(table_name)->second;
  for(auto t:it_index){
    index_id.push_back(it->second);
  }
  this->index_names_.erase(table_name);
  for(auto m:index_id){
    this->indexes_.erase(m);
    this->catalog_meta_->index_meta_pages_.erase(m);
  }
  page_id_t meta_page_id_ = this->catalog_meta_->table_meta_pages_.find(table_id)->second;
  buffer_pool_manager_->UnpinPage(meta_page_id_);
  buffer_pool_manager_->DeletePage(meta_page_id_);
  this->catalog_meta_->table_meta_pages_.erase(table_id);
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  auto it = this->index_names_.find(table_name);
  if(it==this->index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  auto it_index = it->second.find(index_name);
  if(it_index==it->second.end()){
    return DB_TABLE_NOT_EXIST;
  }
  index_id_t index_id = it_index->second;
  it->second.erase(index_name);
  this->indexes_.erase(index_id);
  page_id_t meta_page_id_ = this->catalog_meta_->index_meta_pages_.find(index_id)->second;
  buffer_pool_manager_->UnpinPage(meta_page_id_);
  buffer_pool_manager_->DeletePage(meta_page_id_);
  this->catalog_meta_->index_meta_pages_.erase(index_id);
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  this->buffer_pool_manager_->FlushPage(0);
  return DB_SUCCESS;
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
  auto it = this->tables_.find(table_id);
  if(it==this->tables_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  table_info = it->second;
  return DB_SUCCESS;
}
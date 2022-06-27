#ifndef MINISQL_INDEXES_H
#define MINISQL_INDEXES_H

#include <memory>

#include "catalog/table.h"
#include "index/generic_key.h"
#include "index/b_plus_tree_index.h"
#include "record/schema.h"
#include "page/index_roots_page.h"
#include "record/row.h"
class IndexMetadata {
  friend class IndexInfo;
public:
  static IndexMetadata *Create(const index_id_t index_id, const std::string &index_name,
                               const table_id_t table_id, const std::vector<uint32_t> &key_map,
                               MemHeap *heap);

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap);

  inline std::string GetIndexName() const { return index_name_; }

  inline table_id_t GetTableId() const { return table_id_; }

  uint32_t GetIndexColumnCount() const { return key_map_.size(); }

  inline const std::vector<uint32_t> &GetKeyMapping() const { return key_map_; }

  inline index_id_t GetIndexId() const { return index_id_; }

private:
  IndexMetadata() = default;

  explicit IndexMetadata(const index_id_t index_id, const std::string &index_name,
                         const table_id_t table_id, const std::vector<uint32_t> &key_map) {
                           this->index_id_= index_id;
                           this->index_name_ = index_name;
                           this->table_id_ = table_id;
                           this->key_map_ = key_map;
                         }


private:
  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  index_id_t index_id_;
  std::string index_name_;
  table_id_t table_id_;
  std::vector<uint32_t> key_map_;  /** The mapping of index key to tuple key */
};

/**
 * The IndexInfo class maintains metadata about a index.
 */
class IndexInfo {
public:
  using INDEX_KEY_TYPE128 = GenericKey<128>;
  using INDEX_COMPARATOR_TYPE128 = GenericComparator<128>;
  using BP_TREE_INDEX128 = BPlusTreeIndex<INDEX_KEY_TYPE128, RowId, INDEX_COMPARATOR_TYPE128>;
  using INDEX_KEY_TYPE64 = GenericKey<64>;
  using INDEX_COMPARATOR_TYPE64 = GenericComparator<64>;
  using BP_TREE_INDEX64 = BPlusTreeIndex<INDEX_KEY_TYPE64, RowId, INDEX_COMPARATOR_TYPE64>;
  using INDEX_KEY_TYPE32 = GenericKey<32>;
  using INDEX_COMPARATOR_TYPE32 = GenericComparator<32>;
  using BP_TREE_INDEX32 = BPlusTreeIndex<INDEX_KEY_TYPE32, RowId, INDEX_COMPARATOR_TYPE32>;
  using INDEX_KEY_TYPE16 = GenericKey<16>;
  using INDEX_COMPARATOR_TYPE16 = GenericComparator<16>;
  using BP_TREE_INDEX16 = BPlusTreeIndex<INDEX_KEY_TYPE16, RowId, INDEX_COMPARATOR_TYPE16>;
  using INDEX_KEY_TYPE8 = GenericKey<8>;
  using INDEX_COMPARATOR_TYPE8 = GenericComparator<8>;
  using BP_TREE_INDEX8 = BPlusTreeIndex<INDEX_KEY_TYPE8, RowId, INDEX_COMPARATOR_TYPE8>;
  using INDEX_KEY_TYPE4 = GenericKey<4>;
  using INDEX_COMPARATOR_TYPE4 = GenericComparator<4>;
  using BP_TREE_INDEX4 = BPlusTreeIndex<INDEX_KEY_TYPE4, RowId, INDEX_COMPARATOR_TYPE4>;
  static IndexInfo *Create(MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(IndexInfo));
    return new(buf)IndexInfo();
  }

  ~IndexInfo() {
    this->index_->Destroy();
    delete heap_;
  }

  void Init(IndexMetadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager) {
    // Step1: init index metadata and table info
    // Step2: mapping index key to key schema
    // Step3: call CreateIndex to create the index
    // step 1
    this->table_info_ = table_info;
    this->meta_data_ = meta_data;
    // step 2
    this->key_schema_ = Schema::ShallowCopySchema(table_info->GetSchema(),meta_data->GetKeyMapping(),this->heap_);
    // step 3
    this->index_ = CreateIndex(buffer_pool_manager);
  }

  inline Index *GetIndex() { return index_; }

  inline std::string GetIndexName() { return meta_data_->GetIndexName(); }

  inline IndexSchema *GetIndexKeySchema() { return key_schema_; }

  inline MemHeap *GetMemHeap() const { return heap_; }

  inline TableInfo *GetTableInfo() const { return table_info_; }

private:
  explicit IndexInfo() : meta_data_{nullptr}, index_{nullptr}, table_info_{nullptr},
                         key_schema_{nullptr}, heap_(new SimpleMemHeap()) {}

  Index *CreateIndex(BufferPoolManager *buffer_pool_manager) {
    // allocate root page for the index
    IndexRootsPage* index_root_page = reinterpret_cast<IndexRootsPage*>(buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
    buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID);
    index_id_t index_id = this->meta_data_->GetIndexId();
    page_id_t root_page_id;
    if(!index_root_page->GetRootId(index_id,&root_page_id)){
      index_root_page->Insert(index_id,INVALID_PAGE_ID);
    }
    uint32_t key_size = 4;
    std::vector<Column*> cols = key_schema_->GetColumns();
    uint32_t key_len = 0;
    for(auto &it :cols){
      key_len += it->GetSerializedSize();
    }
    assert(key_len<=128);
    Index* res;
    while(key_size<key_len) key_size<<=1;
    switch(key_size){
      case 4: res = ALLOC_P(this->heap_,BP_TREE_INDEX4)(this->meta_data_->GetIndexId(),this->key_schema_,buffer_pool_manager);break;
      case 8: res =  ALLOC_P(this->heap_,BP_TREE_INDEX8)(this->meta_data_->GetIndexId(),this->key_schema_,buffer_pool_manager);break;
      case 16: res = ALLOC_P(this->heap_,BP_TREE_INDEX16)(this->meta_data_->GetIndexId(),this->key_schema_,buffer_pool_manager);break;
      case 32: res = ALLOC_P(this->heap_,BP_TREE_INDEX32)(this->meta_data_->GetIndexId(),this->key_schema_,buffer_pool_manager);break;
      case 64: res = ALLOC_P(this->heap_,BP_TREE_INDEX64)(this->meta_data_->GetIndexId(),this->key_schema_,buffer_pool_manager);break;
      case 128: res = ALLOC_P(this->heap_,BP_TREE_INDEX128)(this->meta_data_->GetIndexId(),this->key_schema_,buffer_pool_manager);break;
      default: return nullptr;break;
    }
    // syn to table heap
  
  // insert the table content to the index
  
  auto it_table_heap_ = this->table_info_->GetTableHeap()->Begin(nullptr);
  if(it_table_heap_==this->table_info_->GetTableHeap()->End()) return res;
  std::vector<Field> first_key_;
  std::vector<RowId> row_id_;
  for(auto &it:this->meta_data_->key_map_){
      first_key_.push_back(*it_table_heap_->GetField(it));
  }
  if(res->ScanKey(Row(first_key_),row_id_,nullptr)) return res;

  while(it_table_heap_!=this->table_info_->GetTableHeap()->End()){
    vector<Field> key_field;
    for(auto &it:this->meta_data_->key_map_){
      key_field.push_back(*it_table_heap_->GetField(it));
    }
    res->InsertEntry(Row(key_field),it_table_heap_->GetRowId(),nullptr);
    ++it_table_heap_;
  }
  return res;
  }

private:
  IndexMetadata *meta_data_;
  Index *index_;
  TableInfo *table_info_;
  IndexSchema *key_schema_;
  MemHeap *heap_;
};

#endif //MINISQL_INDEXES_H

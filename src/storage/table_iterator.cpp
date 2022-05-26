#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(Row* row_,TableHeap* _table_heap_,Transaction* _txn_) :content(row_),table_heap_(_table_heap_),txn_(_txn_)
{}

TableIterator::TableIterator(const TableIterator &other) {
  content = other.content;
  table_heap_ = other.table_heap_;
  txn_ = other.txn_;
}

TableIterator::~TableIterator() {
  // default
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return (table_heap_==itr.table_heap_)&&(content==itr.content);
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return (table_heap_!=itr.table_heap_)||(content!=itr.content);
}

const Row &TableIterator::operator*() {
  assert(*this!=table_heap_->End());
  return *content;
}

Row *TableIterator::operator->() {
  assert(*this!=table_heap_->End());
  return content;
}

TableIterator &TableIterator::operator++() {
  assert(*this!=table_heap_->End());
  BufferPoolManager* buffer_pool_manager_ = table_heap_->buffer_pool_manager_;
  page_id_t page_id = content->GetRowId().GetPageId();
  // uint32_t slot_num = content->GetRowId().GetSlotNum();
  TablePage* page_ptr = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
  assert(page_ptr!=nullptr);
  RowId next_row_id;
  while(!page_ptr->GetNextTupleRid(content->GetRowId(),&next_row_id)){
    // current page's end, next page
    page_id = page_ptr->GetNextPageId();
    if(page_id==INVALID_PAGE_ID){
      content = nullptr;
      txn_ = nullptr;
      return *this;
    }
    page_ptr = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
  }
  // get next row id
  page_ptr->GetTuple(content,table_heap_->schema_,txn_,table_heap_->lock_manager_);
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator it(*this);
  ++(*this);
  return it;
}

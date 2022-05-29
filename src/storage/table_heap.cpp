#include "storage/table_heap.h"
#include "glog/logging.h"
// #define ENABLE_BPM_DEBUG
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  // assert(row.GetSerializedSize()<PAGE_SIZE);

  page_id_t temp_page_id = this->first_page_id_;
  // assert(false);
  TablePage *table_page_ptr = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  assert(table_page_ptr!=nullptr);
  // search an availble table page
  if(table_page_ptr->GetPrevPageId()!=INVALID_PAGE_ID){
    table_page_ptr->Init(temp_page_id,INVALID_PAGE_ID,log_manager_,txn);
  }
  while (1) {
    // assert(false);
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "page id: "<<temp_page_id<<"\n";
      LOG(INFO) << "page id: " <<table_page_ptr->GetPageId()<<"\n";
#endif
    // try to insert
    if(table_page_ptr->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)) break;
    // assert(false);
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) <<"insert to this page false"<<"\n";
#endif
    // check next page
    if (table_page_ptr->GetNextPageId() == INVALID_PAGE_ID) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "NO AVAILABLE TABLE PAGE!\n";
#endif
      // allocate new page
      page_id_t new_page_id;
      TablePage *new_page_ptr = reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(new_page_id));
      // assert(new_page_ptr!=nullptr);
      if (!new_page_ptr) {
#ifdef ENABLE_BPM_DEBUG
        LOG(ERROR) << "FAIL TO ALLOCATE NEW PAGE!\n";
#endif
        return false;
      }
      table_page_ptr->SetNextPageId(new_page_id);
      new_page_ptr->Init(new_page_id, temp_page_id, log_manager_, txn);
      // insert in new page
      // assert(false);
      if (!new_page_ptr->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
#ifdef ENABLE_BPM_DEBUG
        LOG(ERROR) << "UNEXPECTED ERROR!\n";
#endif
        assert(false);
      }
      buffer_pool_manager_->UnpinPage(new_page_id, false);
      return true;
    }
    // search next page
    // assert(false);
    page_id_t temp_id = temp_page_id;
    temp_page_id = table_page_ptr->GetNextPageId();
    table_page_ptr = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(temp_page_id));
    buffer_pool_manager_->UnpinPage(temp_id,true);
  }
  // write in
  return table_page_ptr->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  // return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  page_id_t page_id = rid.GetPageId();
  // uint32_t slot_num = rid.GetSlotNum();
  TablePage *table_page_ptr = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  buffer_pool_manager_->UnpinPage(page_id, true);
  Row temp_row(rid);
  return table_page_ptr->UpdateTuple(row, &temp_row, schema_, txn, lock_manager_, log_manager_);
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  page_id_t page_id = rid.GetPageId();
  // uint32_t slot_num = rid.GetSlotNum();
  TablePage *table_page_ptr = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  table_page_ptr->ApplyDelete(rid,txn,log_manager_);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  page_id_t page_id = first_page_id_,pre;
  TablePage* page_ptr; 
  while(page_id!=INVALID_PAGE_ID)
  {
    page_ptr = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
    pre = page_id;
    page_id = page_ptr->GetNextPageId();
    buffer_pool_manager_->DeletePage(pre);
  }
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) { 
  page_id_t page_id = row->GetRowId().GetPageId();
  // uint32_t slot_num = row->GetRowId().GetSlotNum();
  TablePage *table_page_ptr = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  return table_page_ptr->GetTuple(row,schema_,txn,lock_manager_);

}

TableIterator TableHeap::Begin(Transaction *txn) 
{ 
  page_id_t page_id = first_page_id_;
  TablePage* table_page_ptr;
  RowId temp;
  while(page_id!=INVALID_PAGE_ID){
    table_page_ptr = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if(!table_page_ptr->GetFirstTupleRid(&temp)){
        page_id = table_page_ptr->GetNextPageId();
    }
    else break;
  }
  if(temp.Get()==INVALID_ROWID.Get()) return TableIterator(nullptr,this,txn);
  Row* row_ = new Row(temp);
  // table_page_ptr->GetTuple(row_,schema_,txn,lock_manager_);
  table_page_ptr->GetTuple(row_,schema_,txn,lock_manager_);
  return TableIterator(row_,this,txn); 
}

TableIterator TableHeap::End() 
{ 
  return TableIterator(nullptr,this,nullptr); 
}

#include <algorithm>
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  // this->SetMaxSize(LEAF_PAGE_SIZE);
   this->SetMaxSize(LEAF_PAGE_SIZE);
  this->SetSize(0);
  this->SetPageType(IndexPageType::LEAF_PAGE);
  this->SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return this->next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
    this->next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int i;
  for (i = 0; i < this->GetSize(); ++i) {
    if (comparator(key, key_[i]) == 0) break;
  }
  return i;
}
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndReturnOnlyChild() {
    return 0;
}
/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  //KeyType key{}
  return key_[index];
}
INDEX_TEMPLATE_ARGUMENTS
KeyType& B_PLUS_TREE_LEAF_PAGE_TYPE:: operator[](int index) {
    return this->key_[index];}
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const {return value_[index];}
    /*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
//INDEX_TEMPLATE_ARGUMENTS
//const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
//  // replace with your own code
//  return ;
//}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int find_index;
  // if (find_index != this->GetSize()) return this->GetSize();
  for (find_index = 0; find_index < this->GetSize(); ++find_index) {
    if (comparator(key_[find_index],key) >= 0){
      if(comparator(key_[find_index],key)==0) return GetSize();
      else break;
    } 
  } 
  this->IncreaseSize(1);
  int i;
  for (i = this->GetSize() - 1; i > find_index; i--) key_[i] = key_[i - 1];
  for (i = this->GetSize() - 1; i > find_index; i--) value_[i] = value_[i - 1];
  value_[find_index] = value;
  key_[find_index] = key;
  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient,KeyType &middle_key,BufferPoolManager *buffer_pool_manager){
  ASSERT(false,"no use");
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient,
                                            BufferPoolManager *buffer_pool_manager_ ) {
  int local_size = GetSize() / 2;
  int recipient_size = this->GetSize() - local_size;
  for (int i = local_size; i < this->GetSize(); ++i) {
    recipient->key_[i - local_size] = key_[i];
    recipient->value_[i - local_size] = value_[i];
  }
  recipient->SetSize(recipient_size);
  this->SetSize(local_size);
  recipient->next_page_id_ = this->next_page_id_;
  this->next_page_id_ = recipient->GetPageId();
  //return recipient->key_[0];

}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {

}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const {
  int i;
  for (i = 0; i < this->GetSize(); i++) {
    if (comparator(key_[i], key) == 0) break;
  }
  if (i < this->GetSize()) {
    value = value_[i];
    return true;
  } else
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {int index = KeyIndex(key,comparator);
  if (index == this->GetSize()) return this->GetSize();
  for (int i = index; i < this->GetSize() - 1; ++i) {
    key_[i] = key_[i + 1];
  }
  for (int i = index; i < this->GetSize() - 1; ++i){
    value_[i] = value_[i+1]; 
   }
  this->IncreaseSize(-1);
   return this->GetSize();
 }

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                           BufferPoolManager *buffer_pool_manager_) {
    //recipient->IncreaseSize(this->GetSize());
  for (int i=recipient->GetSize();i<recipient->GetSize()+this->GetSize();++i){
      recipient->value_[i] = this->value_[i - recipient->GetSize()];
      recipient->key_[i] = this->key_[i - recipient->GetSize()];
   }
   recipient->next_page_id_ = this->GetNextPageId();
  recipient->IncreaseSize(this->GetSize());
  buffer_pool_manager_->UnpinPage(this->GetPageId());
  buffer_pool_manager_->DeletePage(this->GetPageId());
}


/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient,
                                                  KeyType &middle_key,BufferPoolManager *buffer_pool_manager_) {
  this->IncreaseSize(1);
  this->key_[this->GetSize() - 1] = recipient->KeyAt(0);
  this->value_[this->GetSize() -1] = recipient->value_[0];
  for (int i = 0; i < recipient->GetSize()-1; ++i) {
    recipient->key_[i] = recipient->key_[i+1];
    recipient->value_[i] = recipient->value_[i+1];
  }
  recipient->IncreaseSize(-1);
  middle_key = recipient->key_[0];
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {

}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient,
                                                   KeyType &middle_key,BufferPoolManager *buffer_pool_manager_) {
    this->IncreaseSize(1);
    int i;
    for (i = this->GetSize()-1;i > 0;--i){
       key_[i] = key_[i-1];    
       value_[i] = value_[i-1];
    }
    key_[0] = recipient->key_[recipient->GetSize()-1];
    value_[0] = recipient->value_[recipient->GetSize()-1];
    recipient->IncreaseSize(-1);
    middle_key = this->key_[0];
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {

}

template
class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template
class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;

template
class BPlusTreeLeafPage<GenericKey<128>, RowId, GenericComparator<128>>;
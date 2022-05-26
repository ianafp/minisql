#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  //this->page_id_ = page_id;
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  // this->SetMaxSize(INTERNAL_PAGE_SIZE);
   this->SetMaxSize(4);
  this->SetSize(0);
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  //this->page_type_ = IndexPageType::INTERNAL_PAGE;
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  //KeyType key{};
  return key_[index];
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const page_id_t &value) { this->value_[index] = value; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {this->key_[index] = key;}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const page_id_t &value) const {
  int res;
  for (res = 0; res < this->GetSize(); ++res) {
    if (value_[res] == value) break;
  }
  return res;
}
/* return key index that first big than input key*/
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key,const KeyComparator& comparator_) const{
  int i;
  for (i = 0; i < this->GetSize(); ++i) {
      if (comparator_(this->KeyAt(i),key)>0) break;
  } 
  return i;
 }
/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
     KeyType& B_PLUS_TREE_INTERNAL_PAGE_TYPE::operator[](int index) {
     return key_[index];
 }
 INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  // replace with your own code
  //page_id_t val{};
  return value_[index];
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, KeyComparator &comparator) const {
  // replace with your own code
  //int i;
  //for (i = 0; i < size_; ++i) {
  //  if (comparator(key, key_[i]) < 0) break;
  //}
  //BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(this->value_[i]);
  //if (child_page->IsLeafPage()) {
  //  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(child_page);
  //  return leaf_page - Lookup(key, value, comparator);
  //} else {
  //  B_PLUS_TREE_INTERNAL_PAGE_TYPE *internal_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child_page);
  //  return internal_page->Lookup(key, value, comparator);
  //}
  return false;
}
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // replace with your own code
  int val;
  for (val = 0; val < this->GetSize(); ++val) {
    if (comparator(key_[val],key)>0) break;
  }
  return val;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {

}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const page_id_t &old_value, const KeyType &new_key,
const page_id_t &new_value) {
int i;
int find_index = ValueIndex(old_value);
for (i = this->GetSize(); i > find_index; --i) {
  key_[i] = key_[i-1];
}
for (i = this->GetSize()+1; i > find_index + 1; --i) {
  value_[i] = value_[i-1];
}
this->IncreaseSize(1);
this->SetKeyAt(find_index,new_key);
this->SetValueAt(find_index+1,new_value);
return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,BufferPoolManager *buffer_pool_manager){
  ASSERT(false,"no use");
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,KeyType & middle_key,
                                                BufferPoolManager *buffer_pool_manager) {
  int i, half_size = this->GetSize() / 2;
  middle_key = this->key_[half_size];
  int recipient_size = this->GetSize() - half_size - 1;
  for(i = 0;i<recipient_size;++i){
    recipient->key_[i] = this->key_[half_size+1+i];
  }
  for(i=0;i<recipient_size+1;++i){
    recipient->value_[i] = this->value_[half_size+1+i];
    BPlusTreePage* temp_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager->FetchPage(this->value_[half_size+1+i]));
    temp_page->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(this->value_[half_size+1+i]);
  }
  this->SetSize(half_size);
  recipient->SetSize(recipient_size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {

}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // replace with your own code
  
  return this->value_[0];
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
    int i;
  // shift page id
  for (i = recipient->GetSize() + 1; i < recipient->GetSize() + this->GetSize() + 2; ++i) {
      recipient->value_[i] = this->value_[i - 1 - recipient->GetSize()];
      B_PLUS_TREE_INTERNAL_PAGE_TYPE *temp_ptr = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(buffer_pool_manager->FetchPage(recipient->value_[i]));
      temp_ptr->SetParentPageId(recipient->GetPageId());
      buffer_pool_manager->UnpinPage(recipient->value_[i]);
   }
  
   for (i = recipient->GetSize() + 1; i < recipient->GetSize() + this->GetSize()+1; ++i) {
    recipient->key_[i] = this->key_[i - 1 - recipient->GetSize()];
  }
   recipient->key_[recipient->GetSize()] = middle_key;
   recipient->IncreaseSize(this->GetSize()+1);
  buffer_pool_manager->DeletePage(this->GetPageId());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient,KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  this->IncreaseSize(1);
  this->key_[this->GetSize() - 1] = middle_key;
  middle_key = recipient->KeyAt(0);
  this->value_[this->GetSize()] = recipient->value_[0];
  for (int i = 0; i < recipient->GetSize() - 1; ++i) {
    recipient->key_[i] = recipient->key_[i + 1];
  }
  for (int i = 0; i < recipient->GetSize(); ++i) {
    recipient->value_[i] = recipient->value_[i + 1];
  }
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *temp_page =
      reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(buffer_pool_manager->FetchPage(value_[GetSize()]));
  temp_page->SetParentPageId(this->GetPageId());
  buffer_pool_manager->UnpinPage(value_[GetSize()]);
  recipient->IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) 
{


}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  this->IncreaseSize(1);
  int i;
  for (i = this->GetSize() - 1; i > 0; --i) {
    key_[i] = key_[i - 1];
  }
  for (i = this->GetSize(); i > 0; --i) {
    value_[i] = value_[i - 1];
  }
  key_[0] = middle_key;
  value_[0] = recipient->value_[recipient->GetSize()];
  middle_key = recipient->KeyAt(recipient->GetSize()-1);
// persist the child page
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *temp_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(
                                                  buffer_pool_manager->FetchPage(value_[0]));
  temp_page->SetParentPageId(this->GetPageId());
  buffer_pool_manager->UnpinPage(value_[0]);
  recipient->IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {

}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;

template class BPlusTreeInternalPage<GenericKey<4>, RowId, GenericComparator<4>>;

template class BPlusTreeInternalPage<GenericKey<8>, RowId, GenericComparator<8>>;

template class BPlusTreeInternalPage<GenericKey<16>, RowId, GenericComparator<16>>;

template class BPlusTreeInternalPage<GenericKey<32>, RowId, GenericComparator<32>>;

template class BPlusTreeInternalPage<GenericKey<64>, RowId, GenericComparator<64>>;
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(int index,B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page,BufferPoolManager* buffer_pool_manager):index_(index),leaf_page_(leaf_page) ,buffer_pool_manager_(buffer_pool_manager){
  if(leaf_page_!=nullptr){
  val_.first = leaf_page_->KeyAt(index_);
  val_.second = leaf_page_->ValueAt(index_);
  }
}
INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(IndexIterator &other){
  this->index_ = other.index_;
  this->val_ = other.val_;
  this->leaf_page_ = other.leaf_page_;
  this->buffer_pool_manager_ = other.buffer_pool_manager_;
}
INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {

}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  ASSERT(leaf_page_!=nullptr,"the end iterator!");
  return val_;
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  ASSERT(leaf_page_!=nullptr, "the end iterator!");
  if(index_<leaf_page_->GetSize()-1){
    index_++;
  }else{
    // find next page
      buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId());
    if(leaf_page_->GetNextPageId()==INVALID_PAGE_ID){
      // end
      index_ = -1;
      leaf_page_ = nullptr;
      buffer_pool_manager_ = nullptr;
      return *this;
    }else {
      //next page
      leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(buffer_pool_manager_->FetchPage(leaf_page_->GetNextPageId())->GetData());
      index_ = 0;
    }
  }
  val_.first = leaf_page_->KeyAt(index_);
  val_.second = leaf_page_->ValueAt(index_);
  return *this;
}
INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE INDEXITERATOR_TYPE::operator++(int) {
  ASSERT(leaf_page_!=nullptr, "the end iterator!");
  IndexIterator res(*this);
  ++(*this);
  return res;
}
INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return (index_==itr.index_&&leaf_page_==itr.leaf_page_&&buffer_pool_manager_==itr.buffer_pool_manager_);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  return !((*this)==itr);
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;

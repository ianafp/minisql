#include "index/b_plus_tree.h"
#include <string>
#include   <unordered_map>
#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  IndexRootsPage* index_root_page = reinterpret_cast<IndexRootsPage*>(buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID));
  buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID);
  if(!index_root_page->GetRootId(this->index_id_,&this->root_page_id_)){
    this->root_page_id_ = INVALID_PAGE_ID;
  }
  // this->first_page_id_ = INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  // delete in the root page
  IndexRootsPage* root_page_index_ = reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID);
  root_page_index_->Delete(this->index_id_);
  // delete tree
  if(this->root_page_id_==INVALID_PAGE_ID) return;
  BPlusTreePage* tree_page_ = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(this->root_page_id_)->GetData());
  if(tree_page_->IsLeafPage()){
    buffer_pool_manager_->UnpinPage(root_page_id_);
    buffer_pool_manager_->DeletePage(root_page_id_);
  }else {
    reinterpret_cast<InternalPage*>(tree_page_)->destroy(buffer_pool_manager_);
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return (this->root_page_id_ == INVALID_PAGE_ID); }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  ValueType temp;
  if (IsEmpty()) return false;
  BPlusTreePage *root_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData() );
  bool is_leaf = root_page->IsLeafPage();
  if (is_leaf) {
    bool res = reinterpret_cast<LeafPage *>(root_page)->Lookup(key, temp, comparator_);
    buffer_pool_manager_->UnpinPage(root_page_id_);
    result.push_back(temp);
    return res;
  } else {
    BPlusTreePage *temp_internal_page = reinterpret_cast<BPlusTreePage *>(root_page);
    // find the leaf
    while (!temp_internal_page->IsLeafPage()) {
      buffer_pool_manager_->UnpinPage(temp_internal_page->GetPageId());
      auto temp = reinterpret_cast<InternalPage*>(temp_internal_page);
      int child_index = temp->KeyIndex(key, comparator_);
      temp_internal_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(
          buffer_pool_manager_->FetchPage(temp->ValueAt(child_index))->GetData() );
    }
    // find leaf
    LeafPage *res_leaf = reinterpret_cast<LeafPage *>(temp_internal_page);
    bool res = res_leaf->Lookup(key, temp, comparator_);
    result.push_back(temp);
    buffer_pool_manager_->UnpinPage(res_leaf->GetPageId());
    return res;
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (this->root_page_id_ == INVALID_PAGE_ID) {
    LeafPage *root_leaf_page =
        reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(this->root_page_id_)->GetData() );
    root_leaf_page->Init(root_page_id_);
    this->first_page_id_ = root_page_id_;
    root_leaf_page->Insert(key, value, comparator_);
    // success
    buffer_pool_manager_->UnpinPage(this->root_page_id_);
    // update root page
    UpdateRootPageId();
    return true;
  } else
    return InsertIntoLeaf(key, value);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // find the leaf by iterate
  // int i;
  int new_page_id;
  BPlusTreePage* btree_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(root_page_id_));
  bool is_root_leaf = btree_page->IsLeafPage();
  if (is_root_leaf) {
    // leaf page, insert immediately

    // check if exist
    LeafPage *root_leaf_page = reinterpret_cast<LeafPage *>(btree_page);
    
    if (root_leaf_page->KeyIndex(key, comparator_) != root_leaf_page->GetSize()) {
      // already exist
      buffer_pool_manager_->UnpinPage(root_leaf_page->GetPageId());
      return false;
    }
    if (root_leaf_page->Insert(key, value, comparator_) >= root_leaf_page->GetMaxSize()) {
      // need to split
      KeyType temp;
      LeafPage *new_leaf_page = Split<LeafPage>(root_leaf_page, temp);
      InternalPage *new_root_page = reinterpret_cast<InternalPage*>(buffer_pool_manager_->NewPage(new_page_id) ->GetData());
      new_root_page->Init(new_page_id);
      this->root_page_id_ = new_page_id;
      new_leaf_page->SetParentPageId(new_page_id);
      root_leaf_page->SetParentPageId(new_page_id);
      // new_root_page->InsertNodeAfter(INVALID_PAGE_ID, root_leaf_page->KeyAt(0),root_leaf_page->GetPageId());
      new_root_page->SetValueAt(0, root_leaf_page->GetPageId());
      new_root_page->InsertNodeAfter(root_leaf_page->GetPageId(), new_leaf_page->KeyAt(0), new_leaf_page->GetPageId());
      // this->InsertIntoParent(root_leaf_page, new_leaf_page->KeyAt(0),new_leaf_page);
      //  end insert unpin
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      buffer_pool_manager_->UnpinPage(root_leaf_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);

      // update root page id
      UpdateRootPageId();
    }
    return true;
  }
  // then consider the root is internal page, we have to first find the leaf.
  BPlusTreePage *temp_ptr_internal = reinterpret_cast<BPlusTreePage*>(btree_page);
  while (!temp_ptr_internal->IsLeafPage()) {
    // find child page index
    auto temp = reinterpret_cast<InternalPage*>(temp_ptr_internal);
    int key_index = temp->KeyIndex(key, comparator_);
    buffer_pool_manager_->UnpinPage(temp_ptr_internal->GetPageId());
    temp_ptr_internal = reinterpret_cast<BPlusTreePage *>(
        buffer_pool_manager_->FetchPage(temp->ValueAt(key_index))->GetData() );
  }
  // find the leaf
  LeafPage *leaf_page_ = reinterpret_cast<LeafPage *>(temp_ptr_internal);
  // insert to the leaf
  if (leaf_page_->Insert(key, value, comparator_) >= leaf_page_->GetMaxSize()) {
    // the leaf if full, split
    KeyType temp;
    LeafPage *new_leaf_ = Split<LeafPage>(leaf_page_, temp);
    // insert to parent
    InsertIntoParent(leaf_page_,new_leaf_->KeyAt(0),new_leaf_);

    buffer_pool_manager_->UnpinPage(new_leaf_->GetPageId());
  }
  // insert successful
  buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId());
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node, KeyType &middle_key) {
  int new_page_id;
  N *new_page = reinterpret_cast<N *>(buffer_pool_manager_->NewPage(new_page_id)->GetData() );
  // if(new_page_id==node->GetPageId()){
  //   std::cout<<"repeat page: "<<new_page_id<<std::endl;
  //   assert(false);
  // }
  // assert(new_page_id!=node->GetPageId());
  assert(new_page_id!=node->GetPageId());
  new_page->Init(new_page_id, node->GetParentPageId());
  // assert(new_page->GetPageId()!=node->GetPageId());
  if (node->IsLeafPage())
    node->MoveHalfTo(new_page, buffer_pool_manager_);
  else {
    node->MoveHalfTo(new_page, middle_key, buffer_pool_manager_);
  }
  // assert(new_page->GetPageId()!=node->GetPageId());
  return new_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  InternalPage *parent_page =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId())->GetData() );

  // insert middle key to parent node
  new_node->SetParentPageId(parent_page->GetPageId());
  if (parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId()) >= parent_page->GetMaxSize()) {
    // internal node full after insert, need to split
    KeyType middle_key;
    InternalPage *new_internal_page = Split<InternalPage>(parent_page, middle_key);
    if (parent_page->IsRootPage()) {
      // need to generate new root
      int new_root_id;
      // new root
      InternalPage *new_root_internal =
          reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(new_root_id)->GetData() );
      new_root_internal->Init(new_root_id);
      this->root_page_id_ = new_root_id;
      parent_page->SetParentPageId(new_root_id);
      new_internal_page->SetParentPageId(new_root_id);

      new_root_internal->SetValueAt(0, parent_page->GetPageId());
      new_root_internal->InsertNodeAfter(parent_page->GetPageId(), middle_key, new_internal_page->GetPageId());
      buffer_pool_manager_->UnpinPage(new_root_id);
      buffer_pool_manager_->UnpinPage(new_internal_page->GetPageId());

      UpdateRootPageId();
      // end
    } else {
      InsertIntoParent(parent_page, middle_key, new_internal_page);
    }

    // InsertIntoParent(parent_page, new_internal_page->KeyAt(0),new_internal_page);
    buffer_pool_manager_->UnpinPage(new_internal_page->GetPageId());
  }
  // success
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId());
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CheckDeletedPageInTree(BPlusTree& tree)const{
  int i,j,NowCount,NextCount;
  queue<InternalPage*> stack;
  
  BPlusTreePage* temp_page_ptr = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(tree.root_page_id_)->GetData());
  InternalPage* new_ptr;
  buffer_pool_manager_->UnpinPage(tree.root_page_id_);
  if(!temp_page_ptr->IsLeafPage())
    stack.push(reinterpret_cast<InternalPage*>(temp_page_ptr));
  else {
    auto temp = reinterpret_cast<LeafPage*>(temp_page_ptr);
    for(int k =0 ;k < temp->GetSize();++k){
            std::cout<<temp->KeyAt(k)<<" ";
    }
  }
  NowCount = 1;
  NextCount = 1;
  while(stack.size()){
    NowCount = NextCount;
    NextCount = 0;
    for(i=0;i<NowCount;++i){
      new_ptr = stack.front();
      stack.pop();
      for(j=0;j<new_ptr->GetSize()+1;++j){
        temp_page_ptr = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(new_ptr->ValueAt(j))->GetData());
        assert(temp_page_ptr->GetParentPageId()==new_ptr->GetPageId());
        assert(temp_page_ptr->GetPageId()!=temp_page_ptr->GetParentPageId());
        buffer_pool_manager_->UnpinPage(temp_page_ptr->GetPageId());
        if(!temp_page_ptr->IsLeafPage()){
          NextCount ++;
          stack.push(reinterpret_cast<InternalPage*>(temp_page_ptr));
        }else{
          auto temp = reinterpret_cast<LeafPage*>(temp_page_ptr);
          for(int k =0 ;k < temp->GetSize();++k){
            std::cout<<temp->KeyAt(k)<<" ";
          }
        }
      }
    }
  }
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // check if empty
  if (IsEmpty()) return;
  // need to find the leaf contain the key
  
  BPlusTreePage *temp_internal_ptr =
      reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(this->root_page_id_)->GetData() );

  int child_index;
  while (!temp_internal_ptr->IsLeafPage()) {
    // unpin this page
    
    // seek for child
    InternalPage* temp = reinterpret_cast<InternalPage*>(temp_internal_ptr);
    child_index = temp->KeyIndex(key, comparator_);
    buffer_pool_manager_->UnpinPage(temp_internal_ptr->GetPageId());
    temp_internal_ptr = reinterpret_cast<BPlusTreePage *>(
        buffer_pool_manager_->FetchPage(temp->ValueAt(child_index))->GetData() );
  }
  // find leaf
  LeafPage *target_leaf = reinterpret_cast<LeafPage *>(temp_internal_ptr);
  int size_after_remove = target_leaf->RemoveAndDeleteRecord(key, comparator_);

  if (target_leaf->IsRootPage() && size_after_remove == 0) {
    // delete to a empty tree
    buffer_pool_manager_->UnpinPage(this->root_page_id_);
    buffer_pool_manager_->DeletePage(this->root_page_id_);
    this->root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return;
  }
  // leaf node no root
  if (size_after_remove >= target_leaf->GetMinSize()){ 
    buffer_pool_manager_->UnpinPage(target_leaf->GetPageId());
    return;
  }
  // now consider the node need redistribute or coalesce
  CoalesceOrRedistribute<LeafPage>(target_leaf);
  buffer_pool_manager_->UnpinPage(target_leaf->GetPageId());
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::isDistinct(N* node) {
//   for (int i = 0; i < node->GetSize() -1 ; ++i) {
//     if (comparator_(node->KeyAt(i),node->KeyAt(i+1))==0){return false;}
//  }
  for(int i=0;i<node->GetSize()+1;++i){
    if(node->ValueAt(i)==0) return false;
  }
  return true;
}
 INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    // update root
    int old_root_id = this->root_page_id_;
    this->root_page_id_ = node->RemoveAndReturnOnlyChild();
    assert(old_root_id!=root_page_id_);
    BPlusTreePage* temp_tree = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData() );
    temp_tree->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_);
    buffer_pool_manager_->UnpinPage(old_root_id);
    buffer_pool_manager_->DeletePage(old_root_id);
    this->UpdateRootPageId();
    return false;
  }

  InternalPage *parent_page =
      reinterpret_cast<InternalPage*>(reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData()));
  int index = parent_page->ValueIndex(node->GetPageId());
  N *sibling_page;

  if (index == 0)
    sibling_page = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(1))->GetData() );
  
  else
    sibling_page = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(index - 1))->GetData() );
  assert(sibling_page->GetParentPageId()==node->GetParentPageId());
  if (sibling_page->GetSize() > sibling_page->GetMinSize()) {
    Redistribute(sibling_page,node,index);
    //Coalesce<N>(sibling_page, node, parent_page, index);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId());
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId());
    buffer_pool_manager_->UnpinPage(node->GetPageId());
    return false;
    // node->MoveFirstToEndOf(sibling_page, parent_page->value_[index],buffer_pool_manager_);
    //

  if(!parent_page->IsLeafPage())
  for(int i=0;i<parent_page->GetSize()+1;++i){
    auto temp = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(i))->GetData());
    assert(temp->GetPageId());
    assert(temp->GetParentPageId()==parent_page->GetPageId());
    buffer_pool_manager_->UnpinPage(parent_page->ValueAt(i));
  }
  } else {
    // need to merge
    bool recur_;
    if (index == 0) {
      recur_ = Coalesce<N>(node, sibling_page, parent_page, 0);
    } else
      recur_ = Coalesce<N>(sibling_page, node, parent_page, index-1);
    if (recur_) {
      CoalesceOrRedistribute(parent_page);
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId());
    buffer_pool_manager_->UnpinPage(node->GetPageId());
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId());


    return true;
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, const int &index,
                              Transaction *transaction) {
  (node)->MoveAllTo(neighbor_node, (parent)->KeyAt(index), buffer_pool_manager_);
  for (int i = index; i < (parent)->GetSize() - 1; i++) {
    (parent)->SetKeyAt(i, (parent)->KeyAt(i + 1));
  }
  for (int i = index+1; i < (parent)->GetSize(); i++) {
    (parent)->SetValueAt(i, (parent)->ValueAt(i + 1));
  }
  parent->IncreaseSize(-1);
  return ((parent)->GetSize() < (parent)->GetMinSize());
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent_page =
      reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData() );

  // assert(parent_page->GetSize()>index);
  if (index == 0) {
    // index = 0, move sibling's head to this's end
    KeyType &parent_key = (*parent_page)[0];
    node->MoveFirstToEndOf(neighbor_node, parent_key, buffer_pool_manager_);
  } else {
    KeyType &parent_key = (*parent_page)[index-1];
    node->MoveLastToFrontOf(neighbor_node, parent_key, buffer_pool_manager_);
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  if (IsEmpty()) return INDEXITERATOR_TYPE();

  LeafPage *first_leaf =
      reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(this->first_page_id_)->GetData() );
  return INDEXITERATOR_TYPE(0, first_leaf, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  // need to find the leaf page and index in page
  InternalPage *temp_internal_ptr =
      reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData() );
  // find leaf
  while (!temp_internal_ptr->IsLeafPage()) {
    buffer_pool_manager_->UnpinPage(temp_internal_ptr->GetPageId());
    int local_index = temp_internal_ptr->KeyIndex(key, comparator_);
    temp_internal_ptr = reinterpret_cast<InternalPage *>(
        buffer_pool_manager_->FetchPage(temp_internal_ptr->ValueAt(local_index))->GetData() );
  }
  // find leaf
  LeafPage *leaf_page_ = reinterpret_cast<LeafPage *>(temp_internal_ptr);
  int index = leaf_page_->KeyIndex(key, comparator_);
  if (index == leaf_page_->GetSize()) {
    return INDEXITERATOR_TYPE();
  } else {
    return INDEXITERATOR_TYPE(index, leaf_page_, buffer_pool_manager_);
  }
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) { return nullptr; }

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId() {
  IndexRootsPage* meta_root_page = reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID);
  meta_root_page->Update(this->index_id_,this->root_page_id_);
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } 
  else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize() + 1; i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i < inner->GetSize()) {
        out << inner->KeyAt(i);
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    
    // Print leaves
    for (int i = 0; i < inner->GetSize() + 1; i++) {

      BPlusTreePage* child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData() );

      assert(child_page->GetParentPageId()!=child_page->GetPageId());
      ToGraph(child_page, bpm, out);
      
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData() );
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId());
      }
    }
  }


  bpm->UnpinPage(page->GetPageId());
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i)) ), bpm);
      bpm->UnpinPage(internal->ValueAt(i));
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template class BPlusTree<int, int, BasicComparator<int>>;

template class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;

template class BPlusTree<GenericKey<128>, RowId, GenericComparator<128>>;
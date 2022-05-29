#ifndef MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H
#define MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H

#include <queue>
#include "page/b_plus_tree_page.h"

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 28
#define INTERNAL_PAGE_SIZE \
  ((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(ValueType) + sizeof(KeyType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  KeyType KeyAt(int index) const;
  KeyType &operator[](int index);

  void SetValueAt(int index,const page_id_t &value);
  void SetKeyAt(int index, const KeyType &key);
  int KeyIndex(const KeyType &key,const KeyComparator& comparator_)const;
  int ValueIndex(const page_id_t &value) const;

  page_id_t ValueAt(int index) const;
  bool Lookup(const KeyType &key,ValueType& value,KeyComparator& comparator) const;
  page_id_t Lookup(const KeyType &key, const KeyComparator &comparator) const;

  void PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);

  int InsertNodeAfter(const page_id_t &old_value, const KeyType &new_key, const page_id_t &new_value);

  void Remove(int index);
  void destroy(BufferPoolManager* buffer_pool_manager);
  page_id_t RemoveAndReturnOnlyChild();

  // Split and Merge utility methods
  void MoveAllTo(BPlusTreeInternalPage *recipient,const KeyType& middle_key,BufferPoolManager *buffer_pool_manager);

  void MoveHalfTo(BPlusTreeInternalPage *recipient,KeyType &middle_key,BufferPoolManager *buffer_pool_manager);
   void MoveHalfTo(BPlusTreeInternalPage *recipient,BufferPoolManager *buffer_pool_manager);

  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient,KeyType &middle_key,BufferPoolManager* buffer_pool_manafer_);


  void MoveLastToFrontOf(BPlusTreeInternalPage *recipient, KeyType &middle_key,
                         BufferPoolManager *buffer_pool_manager);

private:
  void CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager);

  void CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager);

  void CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager);

   KeyType key_[INTERNAL_PAGE_SIZE];
   page_id_t value_[INTERNAL_PAGE_SIZE + 1];
};

#endif  // MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H

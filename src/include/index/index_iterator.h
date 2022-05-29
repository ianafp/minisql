#ifndef MINISQL_INDEX_ITERATOR_H
#define MINISQL_INDEX_ITERATOR_H

#include "page/b_plus_tree_leaf_page.h"

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator(int index = -1,B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = nullptr,BufferPoolManager* buffer_pool_manager = nullptr);
  IndexIterator(IndexIterator& other);
  ~IndexIterator();

  /** Return the key/value pair this iterator is currently pointing at. */
  const MappingType &operator*();

  /** Move to the next key/value pair.*/
  IndexIterator &operator++();
  IndexIterator operator++(int);
  /** Return whether two iterators are equal */
  bool operator==(const IndexIterator &itr) const;

  /** Return whether two iterators are not equal. */
  bool operator!=(const IndexIterator &itr) const;

private:
  // add your own private member variables here
  int index_;
  std::pair<KeyType,ValueType> val_;
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page_;
  BufferPoolManager* buffer_pool_manager_;
};


#endif //MINISQL_INDEX_ITERATOR_H

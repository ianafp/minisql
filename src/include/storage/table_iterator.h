#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"


class TableHeap;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
  explicit TableIterator(Row* row_,TableHeap* table_heap_,Transaction* txn);

  TableIterator(const TableIterator &other);

  virtual ~TableIterator();

   bool operator==(const TableIterator &itr) const;

   bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  // add your own private member variables here
  Row* content;
  TableHeap* table_heap_;
  Transaction *txn_;
};

#endif //MINISQL_TABLE_ITERATOR_H

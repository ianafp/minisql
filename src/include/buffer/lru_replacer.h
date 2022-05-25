#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>
#include <unordered_map>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;
/**
 * @brief the double linked list node class template
 * 
 * @tparam T 
 */
template<class T>
class list_node{
  public:
  list_node()=default;
  void dequeue();
  public:
  bool isPined;
  T element;
  list_node* next;
  list_node* pre;
};
/**
 * @brief the double linked list class
 * 
 * @tparam T 
 */
template<class T>
class double_linkedlist{
  public:
    double_linkedlist();
    void push_back(list_node<T>& val);
    void push_front(list_node<T>& val);
    T pop_back();
    T pop_front();
    bool isEmpty();
    void dequeue(list_node<T>& to_be_dequeue);
    uint32_t getCount();
    void print();
  private:
    uint32_t count;
    list_node<T> head;
    list_node<T> tail;
    double_linkedlist* next;
    double_linkedlist* pre;
};
/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;
  bool insert(frame_id_t frame_id) override;
  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

private:
  // add your own private member variables here
  uint32_t capacity;
  std::unordered_map<frame_id_t,list_node<frame_id_t>*> lru_hash;
  double_linkedlist<frame_id_t> lru_list;
  double_linkedlist<frame_id_t> pin_list;
  
};

#endif  // MINISQL_LRU_REPLACER_H
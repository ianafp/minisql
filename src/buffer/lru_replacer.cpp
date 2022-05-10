#include "buffer/lru_replacer.h"
// #define ENABLE_BPM_DEBUG
#include "glog/logging.h"
template<class T>
void double_linkedlist<T>::dequeue(list_node<T>& to_be_dequeue)
{
  this->count --;
  to_be_dequeue.pre->next = to_be_dequeue.next;
  to_be_dequeue.next->pre = to_be_dequeue.pre;
  // delete &to_be_dequeue;
}
template<class T>
double_linkedlist<T>::double_linkedlist()
{
  count = 0;
    head.next = &tail;
    head.pre = NULL;
    tail.pre = &head;
    tail.next = NULL;
}
template<class T>
void double_linkedlist<T> ::push_back(list_node<T>& val)
{
  tail.pre->next = &val;
  val.pre = tail.pre;
  val.next = &tail;
  tail.pre = &val;
  count ++;
}
template<class T>
void double_linkedlist<T> ::print()
{
#ifdef ENABLE_BPM_DEBUG
  list_node<T>* temp = head.next;
  int k = 0;
  while(temp!=&tail)
  {
    k++;
    LOG(INFO)<<"("<<k<<","<<temp->element<<")";
    temp = temp->next;
  }
  LOG(INFO)<<"\n";
#endif
}
template<class T>
void double_linkedlist<T> ::push_front(list_node<T>& val)
{
  head.next->pre = &val;
  val.next = head.next;
  val.pre = &head;
  head.next = &val;
  count++;
}
template<class T>
T double_linkedlist<T> ::pop_back()
{
    list_node<T>* temp = tail.pre;
    tail.pre = tail.pre->pre;
    tail.pre->next = &tail;
    count--;
    T res = temp->element;
    delete temp;
    return res;
}
template<class T>
bool double_linkedlist<T>::isEmpty()
{
  return head.next==&tail;
}
template<class T>
T double_linkedlist<T> ::pop_front()
{
    list_node<T>* temp = head.next;
    head.next = head.next->next;
    head.next->pre=head;
    count--;
    T res = temp->element;
    delete temp;
    return res;
}
template <class T>
uint32_t double_linkedlist<T>::getCount(){return count;}
LRUReplacer::LRUReplacer(size_t num_pages):capacity(num_pages) {
}

LRUReplacer::~LRUReplacer() = default;
bool LRUReplacer::insert(frame_id_t frame_id)
{
  if(Size()==capacity) return 0;
  list_node<frame_id_t>* temp = new list_node<frame_id_t>();
  temp->element = frame_id;
  lru_hash.insert(std::pair<frame_id_t,list_node<frame_id_t>*>(frame_id,temp));
  lru_list.push_front(*temp); 
  return 1;
}
bool LRUReplacer::Victim(frame_id_t *frame_id) {
    if(lru_list.isEmpty()) return 0;
    *frame_id = lru_list.pop_back();
    lru_hash.erase(*frame_id);
#ifdef ENABLE_BPM_DEBUG
    LOG(WARNING)<<"the lru list info:\n";
    lru_list.print();
#endif
    return 1;
    
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::unordered_map<frame_id_t,list_node<frame_id_t>*>::iterator it = lru_hash.find(frame_id);
    if(it==lru_hash.end()) {
      // return;
      list_node<frame_id_t>* temp = new list_node<frame_id_t>;
      temp->element = frame_id;
      temp->isPined = 1;
      lru_hash.insert(std::pair<frame_id_t,list_node<frame_id_t>*>(frame_id,temp));
      pin_list.push_back(*temp);
      return;
    }
    if(it->second->isPined==1) return;
    list_node<frame_id_t>* temptr=it->second;
    lru_list.dequeue(*temptr);
    pin_list.push_front(*temptr);
    it->second->isPined = 1;

}

void LRUReplacer::Unpin(frame_id_t frame_id) {
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) <<"IN FUNCTION relpacer.Unpin\n";
#endif
    std::unordered_map<frame_id_t,list_node<frame_id_t>*>::iterator it = lru_hash.find(frame_id);
    if(it==lru_hash.end()) {
      // insert
      list_node<frame_id_t>* temp = new list_node<frame_id_t>;
      temp->element = frame_id;
      temp->isPined = 0;
      lru_hash.insert(std::pair<frame_id_t,list_node<frame_id_t>*>(frame_id,temp));
      lru_list.push_front(*temp);
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) <<"NOT FOUND insert and break\n";
#endif
      return;
    }
    if(it->second->isPined==0) return;
    list_node<frame_id_t>* temptr=it->second;
    pin_list.dequeue(*temptr);
    lru_list.push_front(*temptr);
    it->second->isPined = 0;
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) <<"UNPIN sucessfully break\n";
#endif
}

size_t LRUReplacer::Size() {

  return lru_list.getCount();
}
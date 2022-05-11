##  DISK AND BUFFER POOL MANAGER

作者：张鑫

学号：3200102809

### 1 实验概述

在MiniSQL的设计中，Disk Manager和Buffer Pool Manager模块位于架构的最底层。Disk Manager主要负责数据库文件中数据页的分配和回收，以及数据页中数据的读取和写入。其中，数据页的分配和回收通过位图（Bitmap）这一数据结构实现，位图中每个比特（Bit）对应一个数据页的分配情况，用于标记该数据页是否空闲（`0`表示空闲，`1`表示已分配）。当Buffer Pool Manager需要向Disk Manager请求某个数据页时，Disk Manager会通过某种映射关系，找到该数据页在磁盘文件中的物理位置，将其读取到内存中返还给Buffer Pool Manager。而Buffer Pool Manager主要负责将磁盘中的数据页从内存中来回移动到磁盘，这使得我们设计的数据库管理系统能够支持那些占用空间超过设备允许最大内存空间的数据库。

Buffer Pool Manager中的操作对数据库系统中其他模块是透明的。例如，在系统的其它模块中，可以使用数据页唯一标识符`page_id`向Buffer Pool Manager请求对应的数据页。但实际上，这些模块并不知道该数据页是否已经在内存中还是需要从磁盘中读取。同样地，Disk Manager中的数据页读写操作对Buffer Pool Manager模块也是透明的，即Buffer Pool Manager使用逻辑页号`logical_page_id`向Disk Manager发起数据页的读写请求，但Buffer Pool Manager并不知道读取的数据页实际上位于磁盘文件中的哪个物理页（对应页号`physical_page_id`）。

### 2 实验任务

#### 2.1 位图页

Bitmap Page由两部分组成，一部分是用于加速Bitmap内部查找的元信息（Bitmap Page Meta），它可以包含当前已经分配的页的数量（`page_allocated_`）以及下一个空闲的数据页(`next_free_page_`)，元信息所包含的内容可以由同学们根据实际需要自行定义。除去元信息外，页中剩余的部分就是Bitmap存储的具体数据，其大小`BITMAP_CONTENT_SIZE`可以通过`PAGE_SIZE - BITMAP_PAGE_META_SIZE`来计算，自然而然，这个Bitmap Page能够支持最多纪录`BITMAP_CONTENT_SIZE * 8`个连续页的分配情况。

![image.png](https://cdn.nlark.com/yuque/0/2022/png/25540491/1648371054209-c0dd543c-8ca2-4be0-b0b9-e4a505b0c2de.png)

实现以下功能：

以下函数需要被实现：

- `BitmapPage::AllocatePage(&page_offset)`：分配一个空闲页，并通过`page_offset`返回所分配的空闲页位于该段中的下标（从`0`开始）；
- `BitmapPage::DeAllocatePage(page_offset)`：回收已经被分配的页；
- `BitmapPage::IsPageFree(page_offset)`：判断给定的页是否是空闲（未分配）的。

#### 2.2 磁盘数据页管理

将一个位图页加一段连续的数据页看成数据库文件中的一个分区（Extent），再通过一个额外的元信息页来记录这些分区的信息。通过这种“套娃”的方式，来使磁盘文件能够维护更多的数据页信息。其主要结构如下图所示：

![image.png](https://cdn.nlark.com/yuque/0/2022/png/25540491/1648370611392-3116a928-60ef-4df3-b0fa-5903a431729f.png)

因此，在这个模块中，需要实现以下函数，与之相关的代码位于`src/include/storage/disk_manager.h`和`src/storage/disk_manager.cpp`。

- `DiskManager::AllocatePage()`：从磁盘中分配一个空闲页，并返回空闲页的**逻辑页号**；
- `DiskManager::DeAllocatePage(logical_page_id)`：释放磁盘中**逻辑页号**对应的物理页。
- `DiskManager::IsPageFree(logical_page_id)`：判断该**逻辑页号**对应的数据页是否空闲。
- `DiskManager::MapPageId(logical_page_id)`：可根据需要实现。在`DiskManager`类的私有成员中，该函数可以用于将逻辑页号转换成物理页号。

#### 2.3 LRU替换策略

Buffer Pool Replacer负责跟踪Buffer Pool中数据页的使用情况，并在Buffer Pool没有空闲页时决定替换哪一个数据页。在本节中，你需要实现一个基于LRU替换算法的`LRUReplacer`，`LRUReplacer`类在`src/include/buffer/lru_replacer.h`中被定义，其扩展了抽象类`Replacer`（在`src/include/buffer/replacer.h`中被定义）。`LRUReplacer`的大小默认与Buffer Pool的大小相同。

因此，在这个模块中，需要实现以下函数，与之相关的代码位于`src/buffer/lru_replacer.cpp`中。

- `LRUReplacer::Victim(*frame_id)`：替换（即删除）与所有被跟踪的页相比最近最少被访问的页，将其页帧号（即数据页在Buffer Pool的Page数组中的下标）存储在输出参数`frame_id`中输出并返回`true`，如果当前没有可以替换的元素则返回`false`；

- `LRUReplacer::Pin(frame_id)`：将数据页固定使之不能被`Replacer`替换，即从`lru_list_`中移除该数据页对应的页帧。`Pin`函数应当在一个数据页被Buffer Pool Manager固定时被调用；
- `LRUReplacer::Unpin(frame_id)`：将数据页解除固定，放入`lru_list_`中，使之可以在必要时被`Replacer`替换掉。`Unpin`函数应当在一个数据页的引用计数变为`0`时被Buffer Pool Manager调用，使页帧对应的数据页能够在必要时被替换；
- `LRUReplacer::Size()`：此方法返回当前`LRUReplacer`中能够被替换的数据页的数量。

#### 2.4 缓冲池管理

在实现Buffer Pool的替换算法`LRUReplacer`后，你需要实现整个`BufferPoolManager`，与之相关的代码位于`src/include/buffer/buffer_pool_manager.h`和`src/buffer/buffer_pool_manager.cpp`中。Buffer Pool Manager负责从Disk Manager中获取数据页并将它们存储在内存中，并在必要时将脏页面转储到磁盘中（如需要为新的页面腾出空间）。

需要实现以下函数：

- `BufferPoolManager::FetchPage(page_id)`：根据逻辑页号获取对应的数据页，如果该数据页不在内存中，则需要从磁盘中进行读取；
- `BufferPoolManager::NewPage(&page_id)`：分配一个新的数据页，并将逻辑页号于`page_id`中返回；
- `BufferPoolManager::UnpinPage(page_id, is_dirty)`：取消固定一个数据页；
- `BufferPoolManager::FlushPage(page_id)`：将数据页转储到磁盘中；
- `BufferPoolManager::DeletePage(page_id)`：释放一个数据页；
- `BufferPoolManager::FlushAllPages()`：将所有的页面都转储到磁盘中。

### 3 实验过程

#### 3.1 配置环境

实验环境: WSL Ubuntu 20.04 

将远程仓库clone到wsl环境中

#### 3.2 编译&测试结果

进入/minisql

```shell
mkdir build
cd build
cmake ..
make -j #生成全部测试程序
make disk_manager_test #生成磁盘测试程序
make lru_replacer_test #生成replacer测试程序
make buffer_pool_manager_test #生成内存池测试程序
# 测试时调用/minisql/build/test/下的对应模块的测试可执行文件即可
```

测试结果

![image-20220510225747742](C:\Users\86173\AppData\Roaming\Typora\typora-user-images\image-20220510225747742.png)

![image-20220510225757832](C:\Users\86173\AppData\Roaming\Typora\typora-user-images\image-20220510225757832.png)

（注：红色日志为我使用日志库对buffer_pool_manager执行进行记录）

#### 3.3 位图页策略

位图页使用项目原生BitmapPage类私有成员

```c++
private:
  /** The space occupied by all members of the class should be equal to the PageSize */
  uint32_t page_allocated_;
  uint32_t next_free_page_;
  unsigned char bytes[MAX_CHARS];
```

该类所有方法采用位运算实现

由于一个字节为8bit, 每bit对应一页，所以得到offset操作对应bit的办法是：

```c++
int byte_id = offset << 3; // 找到在哪一个字节
int bit_id = offset & 0x7; // 该字节的高位向地位的第几位
```

基于此：

isPageFree的实现如图:

```c++
template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return !(bytes[page_offset >> 3] & (1 << (7 - (page_offset & 0x7))));
}
```

AllocatePage策略：

首先判断位图页是否满了，是则返回0， 否则将next_free_page分配给返回值

如果分配后位图页未满，进行循环查找得到下一个next_free_page

```c++
template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset)
{
    if (page_allocated_ == MAX_CHARS * 8) return 0;
    page_allocated_++;
    page_offset = next_free_page_;
#ifdef ENABLE_BPM_DEBUG
      LOG(ERROR)<<"OFFSET: "<<page_offset<<"\n";
      if(page_offset>=MAX_CHARS*8)
      {
        LOG(ERROR)<<"OFFSER OVER in ALLOCATEPAGE!\nOFFSET: "<<page_offset<<"\n";
      }
#endif
    bytes[page_offset>>3] |= 1 << (7 - (page_offset & 0x7));
    if(page_allocated_<MAX_CHARS * 8)
    while (true) {
      next_free_page_ = (next_free_page_ + 1) % (MAX_CHARS << 3);
      if (IsPageFree(next_free_page_)) break;
    }
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "next free page : "<<next_free_page_<< std::endl;
#endif
    return 1;
}
```

DeAllocatePage 策略:

首先判断回收页是否被占用，否则返回0， 是则置位

为了防止回收页无法利用，每次回收后将next_free_page指向回收页

```c++
template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
    if (IsPageFree(page_offset)) return 0;
    next_free_page_ = page_offset;
#ifdef ENABLE_BPM_DEBUG
      if(page_offset>=MAX_CHARS*8)
      {
        LOG(ERROR)<<"OFFSER OVER in DEALLOCATEPAGE!\nOFFSET: "<<page_offset<<"\n";
      }
#endif
    page_allocated_--;
    bytes[page_offset >> 3] &= ~(1 << (7 - (page_offset & 0x7)));
    return 1;
}
```

#### 3.4 磁盘管理策略

磁盘页的构成位一页meta，若干分区，每一个分区组成为一页位图页 + 分区最大容量页

实际上，我们每次新建分区并不需要实际上对物理存储进行操作，只是在meta中抽象地实质存在，这样做的机理是工程提供的ReadPhysicalPage方法中，在读取不存在的页时，返回的是初始化的页，与我们的目的正相符合。

首先映射逻辑页到物理页。

```c++
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) << "in function MapPageId" << std::endl;
#endif
  uint32_t extent_capacity = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  uint32_t extent_id = logical_page_id / extent_capacity;
  uint32_t page_id_in_extent = logical_page_id % extent_capacity;
#ifdef ENABLE_BPM_DEBUG
      LOG(ERROR) << "logical: "<<logical_page_id<<" physical: "<< 2+extent_id+extent_id*extent_capacity+page_id_in_extent<< std::endl; 
#endif
  return 2+extent_id+extent_id*extent_capacity+page_id_in_extent;
}
```

AllocatePage策略：

首先查看meta页检查文件是否已满，是则返回invalid_page_id， 否则遍历分区记录找到存在余量的分区

将该分区bitmap读入到内存中进行分配操作后将bitmap写回

```c++
page_id_t DiskManager::AllocatePage() {
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) <<"in function ALLOCATE PAGE\n";
#endif
    DiskFileMetaPage* meta = reinterpret_cast<DiskFileMetaPage*> (meta_data_);
    // check if full
    if(meta->GetAllocatedPages()==MAX_VALID_PAGE_ID) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) <<"the db file is already full! \n";
#endif
      return INVALID_PAGE_ID;
    }
    uint32_t i;
    // max pages of an extent
    uint32_t extent_capacity = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
    // search an available extent
    uint32_t extent_id;
    // check if the exist extent has an available page
    for(i=0;i<meta->GetExtentNums();i++)
    {
      if(meta->GetExtentUsedPage(i)<extent_capacity) break;
    }
    if(i==meta->GetExtentNums())
    {
      // the current extent all full
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "the current extent all full, create new extent\n";
#endif
      // create new extent
      extent_id = meta->num_extents_ ++ ;
    }
    else 
    {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "find the extent id :"<<i<<"\n";
#endif
      extent_id = i;
    }
    // generate meta
    uint32_t bitmap_physical_id = 1 + extent_id * (1 + extent_capacity);
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) <<"!!!the bitmap page : "<<bitmap_physical_id<<"!!!\n";
#endif
    // read in bit map
    char bitmap_contrnt[PAGE_SIZE];
    ReadPhysicalPage(bitmap_physical_id,bitmap_contrnt);
    // create bitmap
    BitmapPage<PAGE_SIZE>* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_contrnt);
    // get page from bitmap
    (meta->extent_used_page_[extent_id])++;
    uint32_t page_id_in_extent;
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) <<"!!!\n";
#endif
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) <<"???the bitmap meta : "<<bitmap->next_free_page_<<"???\n";
#endif
    bitmap->AllocatePage(page_id_in_extent);
    // write back
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) <<"###the bitmap meta : "<<bitmap->next_free_page_<<"###\n";
      LOG(WARNING) <<"###the bitmap page : "<<bitmap_physical_id<<"###\n";
#endif
    WritePhysicalPage(bitmap_physical_id,bitmap_contrnt);
    // WritePhysicalPage(0,meta_data_);
    // return
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) <<"break ALLOCATE PAGE\n";
#endif

    return extent_capacity * extent_id + page_id_in_extent;
}
```

#### 3.5 lru替换策略

采用hash链表结构实现lru替换，机理如下：

使用hash表使得我们能够映射页帧到其在替换列表中的位置，近而以$O(1)$时间进行替换

使用双链表结构实现pin_list和lru_list，分别为pin页和未pin的页的双链表

在lru_list中，离dummy_head越近，表示其越活跃，离dummy_tail越近，表示其越废弃，因而每次插入向头部插入，替换则取出尾部元素，同时更新hash表

```c++
private:
  // add your own private member variables here
  uint32_t capacity;
  std::unordered_map<frame_id_t,list_node<frame_id_t>*> lru_hash;
  double_linkedlist<frame_id_t> lru_list;
  double_linkedlist<frame_id_t> pin_list;
  
};
```

双链表定义如下

```c++
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
```

victim:

```c++
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
```

pin:

```c++
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

```

unpin:

```c++
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

```

size:

```c++
size_t LRUReplacer::Size() {

  return lru_list.getCount();
}
```

#### 3.6 内存池策略

在内存池中，初始化时所有页都在free_list_中，每次我们从内存读取页，都从freelist中分配叶帧，更新page_id到页帧的映射，将其置入replacer，当free list为空时，我们从replacer中进行替换，再放回replacer中，直到所有页都被固定，分配失败。

当新建页时，除了分配页帧外，还应将其固定。

删除页时，将其置入free_list,更新映射。

pin和unpin则直接调用replacer模块方法。

FetchPage:

```c++
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::unordered_map<page_id_t, frame_id_t>::iterator it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // the page is not in buffer, need to fetch from disk
    // first to check if there is free frame
    frame_id_t victim;
    if (free_list_.empty()) {
      // need to replace a page

      if (!replacer_->Victim(&victim)) return nullptr;
      if (pages_[victim].IsDirty()) {
        // dirty page, write back
        disk_manager_->WritePage(pages_[victim].page_id_, pages_[victim].data_);
      }
      // erase from page table
      page_table_.erase(pages_[victim].page_id_);

    } else {
      // just use free page()
      victim = free_list_.back();
      free_list_.pop_back();
      // register in map
      page_table_.insert(std::pair<page_id_t, frame_id_t>(page_id, victim));
      // insert to replacer
      if (!replacer_->insert(victim)) {
        // replacer is full , replace instead
        frame_id_t local_temp;
        replacer_->Victim(&local_temp);
        // write back if necessary
        if (pages_[local_temp].IsDirty()) {
          // dirty page, write back
          disk_manager_->WritePage(pages_[local_temp].page_id_, pages_[local_temp].data_);
        }
        replacer_->insert(victim);
      }
    }
    // reset metadata
    pages_[victim].is_dirty_ = false;
    pages_[victim].page_id_ = page_id;
    pages_[victim].pin_count_ = 0;
    // read in from disk
    disk_manager_->ReadPage(page_id, pages_[victim].data_);
    // register in map
    page_table_.insert(std::pair<page_id_t, frame_id_t>(page_id, victim));
    // return
    return pages_ + victim;
  } else {
    replacer_->Pin(it->second);
    return pages_ + it->second;
  }
}


```

NewPage:

```c++
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) <<"IN FUNCTION bufferpool.NewPage\n";
#endif
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) <<"disk allocate page successfully\n";
#endif
  frame_id_t victim;
  if (!free_list_.empty()) {
#ifdef ENABLE_BPM_DEBUG
  LOG(INFO)<<"the free list is not empty\n";
#endif
    victim = free_list_.front();
    free_list_.pop_front();
    //
  } else {
#ifdef ENABLE_BPM_DEBUG
  LOG(INFO)<<"the free list  not empty, victim from replacer\n";
#endif
    if (!replacer_->Victim(&victim)) {
      LOG(ERROR)<<"NO REPLACE PAGE\n";
      return nullptr;
    }
  
    // check if write back
    if(pages_[victim].IsDirty())
    {
      disk_manager_->WritePage(pages_[victim].page_id_,pages_[victim].data_);
    }
    // erase 
    page_table_.erase(pages_[victim].page_id_);
  }
#ifdef ENABLE_BPM_DEBUG
  LOG(INFO)<<"victim: "<<victim<<"\n";
#endif
  page_id = disk_manager_->AllocatePage();
  if(page_id==INVALID_PAGE_ID) return nullptr;
  // replacer_->Unpin(victim);
  replacer_->Pin(victim);
  page_table_.insert(std::pair<page_id_t,frame_id_t>(page_id,victim));
  pages_[victim].ResetMemory();
  pages_[victim].is_dirty_ = false;
  pages_[victim].page_id_ = page_id;
  pages_[victim].pin_count_ = 0;
  return pages_+victim;
}
```

DeletePage:

```c++
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  // delete page
  disk_manager_->DeAllocatePage(page_id);
  // search
  std::unordered_map<page_id_t,frame_id_t>::iterator it = page_table_.find(page_id);
  if(it==page_table_.end()) return 1;
  frame_id_t frame_id = it->second;
  if(pages_[frame_id].pin_count_) return 0;
  // remove
  page_table_.erase(page_id);
  // reset
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  // add to free list
  free_list_.push_back(frame_id);
  return true;
}
```

unpin

```c++
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) { 
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) << "IN FUNCTION unpinpage:" << std::endl;
#endif
  std::unordered_map<page_id_t,frame_id_t>::iterator it = page_table_.find(page_id);
  if(it==page_table_.end()) {
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) << "break function UNPINPAGE: NOT FOUND THIS PAGE IN BUFFER" << std::endl;
#endif
    return 0;
  }
  pages_[it->second].is_dirty_ = is_dirty;
  replacer_->Unpin(it->second);
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) << "break function UNPINPAGE: unpin successfully" << std::endl;
#endif
  return 1;
}
```

FlushPage:

```c++
bool BufferPoolManager::FlushPage(page_id_t page_id) {
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) << "IN FUNCTION BufferPoolManager.FlushPage" << std::endl;
#endif
  std::unordered_map<page_id_t,frame_id_t>::iterator it = page_table_.find(page_id);
  if(it==page_table_.end()) {
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) << "page not exist , break funtion" << std::endl;
#endif
    return 0;
  }
  disk_manager_->WritePage(page_id,pages_[it->second].data_);
#ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) << "flush successfully , break funtion" << std::endl;
#endif
  return 1;
}
```


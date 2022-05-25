#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"
// #define ENABLE_BPM_DEBUG
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  // assert(false);
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

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
    replacer_->Pin(victim);
    return pages_ + victim;
  } else {
    replacer_->Pin(it->second);
    return pages_ + it->second;
  }
}

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

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) { disk_manager_->DeAllocatePage(page_id); }

bool BufferPoolManager::IsPageFree(page_id_t page_id) { return disk_manager_->IsPageFree(page_id); }

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}
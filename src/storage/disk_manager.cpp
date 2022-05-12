#include <stdexcept>
#include <sys/stat.h>
// #define ENABLE_BPM_DEBUG
#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"
DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  // std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  // std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  // assert(false);
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

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

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
    // generate meta
    DiskFileMetaPage* meta = reinterpret_cast<DiskFileMetaPage*> (meta_data_);
    if(logical_page_id>=MAX_VALID_PAGE_ID) return ;
    uint32_t extent_capacity = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
    uint32_t extent_id = logical_page_id / extent_capacity;
    char bitmap_content[PAGE_SIZE];
    uint32_t bitmap_physical_id = 1 + extent_id * (1 + extent_capacity);
    ReadPhysicalPage(bitmap_physical_id,bitmap_content);
    BitmapPage<PAGE_SIZE>* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_content);    
    uint32_t page_id_in_extent = logical_page_id % extent_capacity;
    if(bitmap->DeAllocatePage(page_id_in_extent))
    {
      meta->extent_used_page_[extent_id]--;
    }
    // write back

    WritePhysicalPage(bitmap_physical_id,bitmap_content);
    // WritePhysicalPage(0,meta_data_);
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  uint32_t extent_capacity = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  uint32_t extent_id = logical_page_id / extent_capacity;
  uint32_t bitmap_physical_id = 1 + extent_id * (1 + extent_capacity);
  char bitmap_content[PAGE_SIZE];
  ReadPhysicalPage(bitmap_physical_id,bitmap_content);
  BitmapPage<PAGE_SIZE>* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_content);  
  return bitmap->IsPageFree(logical_page_id % extent_capacity);
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  // assert(false);
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

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "read over" << std::endl;
#endif
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
#ifdef ENABLE_BPM_DEBUG
  LOG(INFO) <<"write successfully\n";
#endif
}
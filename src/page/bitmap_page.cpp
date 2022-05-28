#include "page/bitmap_page.h"
#include "glog/logging.h"
// #define ENABLE_BPM_DEBUG
template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset)
{
    if (page_allocated_ == MAX_CHARS * 8) return 0;
    page_allocated_++;
    page_offset = next_free_page_;

    bytes[page_offset>>3] |= 1 << (7 - (page_offset & 0x7));
    if(page_allocated_<MAX_CHARS * 8)
    while (true) {
      next_free_page_ = (next_free_page_ + 1) % (MAX_CHARS << 3);
      if (IsPageFree(next_free_page_)) break;
    }

    return 1;
}
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

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return !(bytes[page_offset >> 3] & (1 << (7 - (page_offset & 0x7))));
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return bytes[byte_index] & (1<<(7-bit_index));
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;
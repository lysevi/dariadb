#pragma once
namespace dariadb {
namespace storage {
namespace memstorage {
struct Description {
  size_t allocated;
  size_t allocator_capacity;
  Description() { allocated = allocator_capacity = size_t(0); }
};
}
}
}
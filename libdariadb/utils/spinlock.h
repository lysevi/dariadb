#pragma once

#include <atomic>
#include <mutex> //for lock_guard

namespace dariadb{
    namespace utils{
        class SpinLock {
            std::atomic_flag locked = ATOMIC_FLAG_INIT ;
        public:
            void lock() {
                while (locked.test_and_set(std::memory_order_acquire)) { ; }
            }
            void unlock() {
                locked.clear(std::memory_order_release);
            }
        };
    }
}

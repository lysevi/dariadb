#pragma once

#include <atomic>
#include <mutex> //for lock_guard
#include <memory>

namespace dariadb{
    namespace utils{
        //using Locker=std::mutex;

        class Locker {
            std::atomic_flag locked = ATOMIC_FLAG_INIT ;
        public:
            void lock() {
                while (locked.test_and_set(std::memory_order_acquire)) { ; }
            }
            void unlock() {
                locked.clear(std::memory_order_release);
            }
        };

        using Locker_ptr=std::shared_ptr<dariadb::utils::Locker>;
    }
}

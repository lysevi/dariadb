#pragma once

#include <atomic>
#include <mutex> //for lock_guard

namespace dariadb{
    namespace utils{
        class Locker {
#ifndef USE_MUTEX
            std::atomic_flag locked = ATOMIC_FLAG_INIT ;
#else
			std::mutex locked;
#endif
        public:
            void lock() {
#ifndef USE_MUTEX
                while (locked.test_and_set(std::memory_order_acquire)) { ; }
#else
				locked.lock();
#endif
            }
            void unlock() {
#ifndef USE_MUTEX
                locked.clear(std::memory_order_release);
#else
				locked.unlock();
#endif
            }
        };
    }
}

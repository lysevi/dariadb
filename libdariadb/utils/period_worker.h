#pragma once

#include <thread>
#include <condition_variable>
#include <queue>
#include <mutex>
#include <assert.h>
#include <atomic>
#include <chrono>

namespace dariadb {
	namespace utils {

		// look usage example in utils_test.cpp
		class PeriodWorker {
		public:
			PeriodWorker(const std::chrono::seconds sleep_time) {
				_sleep_time = sleep_time;
			}
			virtual ~PeriodWorker() {
				if (m_thread_work) {
					this->kill();
				}
			}

			virtual void call() = 0;

			void start() {
				m_stop_flag = false;
				m_thread = std::thread(&PeriodWorker::_thread_func, this);
				assert(m_thread.joinable());
			}

			void kill() {
				m_stop_flag = true;
				m_thread.join();
			}

			/// whait, while all works done and stop thread.
			void stop() {
				m_stop_flag = true;
				m_thread.join();
			}

			bool stoped() const { return m_stop_flag; }

		protected:
			void _thread_func() {
				std::unique_lock<std::mutex> lock(m_thread_lock);
				m_thread_work = true;
				while (!m_stop_flag) {
					std::this_thread::sleep_for(_sleep_time);
					this->call();
				}
				m_thread_work = false;
			}

		private:
			mutable std::mutex m_add_lock, m_thread_lock;
			std::thread m_thread;
			std::atomic<bool> m_stop_flag, m_thread_work;
			std::chrono::seconds _sleep_time;
		};
	}
}

#pragma once

#include <thread>
#include <condition_variable>
#include <queue>
#include <mutex>
#include <assert.h>
#include "locker.h"

namespace dariadb{
    namespace utils {

        // look usage example in utils_test.cpp
        template <class T> class AsyncWorker {
        public:
            virtual ~AsyncWorker(){
                if(!_is_stoped){
                    this->stop_async();
                }
            }

            /// add data to queue for processing
            void add_async_data(const T& data) {
                std::unique_lock<Locker> lg(_locker);
                _in_queue.push(data);
                _data_cond.notify_one();
            }

            /// wait, while have data
            void  flush_async() {
				//TODO refact this (use conditinal varables or mutexes)
                const std::chrono::milliseconds sleep_time = std::chrono::milliseconds(100);
                while (!this->_in_queue.empty()) {
                    std::this_thread::sleep_for(sleep_time);
                }
            }

            virtual void call_async(const T&) { assert(false); }

            ///worker start
            void start_async() {
                _is_stoped=false;
                _write_thread_stop = false;
                _write_thread_handle = std::move(std::thread{ &AsyncWorker<T>::_thread_func,this });
            }

            /// whait, while all works done and stop thread.
            void stop_async() {
                if (!stoped()) {
                    this->flush_async();

                    _write_thread_stop = true;
                    _data_cond.notify_one();
                    _write_thread_handle.join();
                    _is_stoped=true;
                }
            }

            bool stoped()const {return _is_stoped;}

            bool is_busy() const { return !_in_queue.empty(); }

            size_t async_queue_size()const{return this->_in_queue.size();}
        protected:
            void _thread_func() {
				std::mutex local_lock;

                while (!_write_thread_stop) {
					std::unique_lock<std::mutex> lk(local_lock);
                    _data_cond.wait(lk, [&] {return !_in_queue.empty() || _write_thread_stop; });
                    while (!_in_queue.empty()) {
                        auto d = _in_queue.front();
						_locker.lock();
                        _in_queue.pop();
						_locker.unlock();
                        this->call_async(d);
                    }
                }
            }

        private:
            bool _is_stoped;
            mutable  Locker  _locker;
            std::queue<T> _in_queue;
            bool        _write_thread_stop;
            std::thread _write_thread_handle;
            std::condition_variable _data_cond;
        };
    }
}

#pragma once

#include <atomic>
#include "../utils/locker.h"

namespace dariadb {
namespace storage {

const size_t AOF_BUFFER_SIZE = 1000;

class Options {
    Options(){
        aof_buffer_size=AOF_BUFFER_SIZE;
    }
    ~Options()=default;
public:
    static void start();
    static void stop();
    static Options* instance(){
        return _instance;
    }


    std::string path;
    uint64_t aof_max_size;  // measurements count in one file
    size_t   aof_buffer_size; // inner buffer size
private:
    static Options *_instance;
};
}
}

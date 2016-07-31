#pragma once

#include <atomic>
#include "../utils/locker.h"

namespace dariadb {
namespace storage {

class Options {
    Options()=default;
    ~Options()=default;
public:
    static void start();
    static void stop();
    static Options* instance(){
        return _instance;
    }
private:
    static Options *_instance;
};
}
}

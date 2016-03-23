#pragma once

#include "utils.h"

namespace dariadb{
    namespace storage{
        class PageManager:public utils::NonCopy {
        public:
            PageManager();
            static void start();
            static void stop();
            static PageManager* instance();
        private:
            static PageManager*_instance;
        };
    }
}

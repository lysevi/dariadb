#pragma once

#include <memory>

namespace dariadb{
    namespace net{
        class Client
        {
        public:
            Client();
            ~Client();
        protected:
            class Private;
            std::unique_ptr<Private> _Impl;
        };
    }
}

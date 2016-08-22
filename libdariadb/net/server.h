#pragma once

#include <memory>

namespace dariadb{
    namespace net{
        class Server
        {
        public:
            Server();
            ~Server();
        protected:
            class Private;
            std::unique_ptr<Private> _Impl;
        };
    }
}

#pragma once


namespace dariadb{
    namespace net{
        class IClientManager{
        public:
            virtual void client_connect(int id)=0;
            virtual void client_disconnect(int id)=0;
        };
    }
}

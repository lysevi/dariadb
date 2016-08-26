#pragma once

#include <string>
#include <ostream>

namespace dariadb{
    namespace net{
        const std::string OK_ANSWER="OK";
        const std::string ERROR_ANSWER="error";
        const std::string HELLO_PREFIX="hello";
        const std::string DISCONNECT_PREFIX="bye";
        const std::string DISCONNECT_ANSWER="bye";
        const std::string PING_QUERY="ping";
        const std::string PONG_ANSWER="pong";

        enum class ClientState{
          CONNECT, //connection is beginning but a while not ended.
          WORK, //normal client.
          DISCONNECTED
        };

        std::ostream &operator<<(std::ostream &stream, const ClientState &state);
    }
}

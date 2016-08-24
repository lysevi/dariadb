#include "net_common.h"
#include <istream>
#include <boost/asio.hpp>

namespace dariadb{
namespace net{
    std::ostream &operator<<(std::ostream &stream, const ClientState &state){
        switch(state){
        case dariadb::net::ClientState::WORK:
            stream<<"ClientState::WORK";
            break;
        case dariadb::net::ClientState::DISCONNECTED:
            stream<<"ClientState::DISCONNECTED";
            break;
        case dariadb::net::ClientState::CONNECT:
            stream<<"ClientState::CONNECT";
            break;
        }
        return stream;
    }
}
}

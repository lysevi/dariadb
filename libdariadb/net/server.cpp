#include "server.h"
#include <boost/asio.hpp>

using namespace dariadb;
using namespace dariadb::net;

class Server::Private{
public:
    Private(){

    }
};


Server::Server():_Impl(new Server::Private()){

}


Server::~Server(){

}

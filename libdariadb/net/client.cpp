#include "client.h"
#include <boost/asio.hpp>

using namespace dariadb;
using namespace dariadb::net;

class Client::Private{
public:
    Private(){

    }
};


Client::Client():_Impl(new Client::Private()){

}


Client::~Client(){

}

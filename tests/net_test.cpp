#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <boost/asio/steady_timer.hpp>
#include <net/client.h>
#include <net/server.h>
#include <thread>
#include <atomic>
#include <iostream>
#include <chrono>

const dariadb::net::Server::Param server_param(2001);
const dariadb::net::Client::Param client_param("127.0.0.1", 2001);

std::atomic_bool server_runned;
std::atomic_bool server_stop_flag;
dariadb::net::Server *server_instance=nullptr;
void server_thread_func(){
    dariadb::net::Server s(server_param);

    while(!s.is_runned()){}

    server_instance=&s;
    server_runned.store(true);
    while(!server_stop_flag.load()){}

    server_instance=nullptr;
}

BOOST_AUTO_TEST_CASE(Connect) {
    server_runned.store(false);
    std::thread server_thread{server_thread_func};

    while(!server_runned.load()){
    }

    dariadb::net::Client c(client_param);
    c.connect();

    while(true){
        auto res=server_instance->connections_accepted();
        if(res==size_t(1)){
            break;
        }
    }
    server_stop_flag=true;
    server_thread.join();
}

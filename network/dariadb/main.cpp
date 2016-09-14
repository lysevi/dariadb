#include <libdariadb/engine.h>
#include <libdariadb/meas.h>
#include <libdariadb/utils/logger.h>
#include <libserver/server.h>

const dariadb::net::Server::Param server_param(2001);


int main(int argc,char**argv){

	dariadb::net::Server s(server_param);

	s.start();

	while (!s.is_runned()) {
	}

	
	while (s.is_runned()){
		std::this_thread::yield();
	}
}
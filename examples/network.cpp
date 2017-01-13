#include <libdariadb/engine.h>
#include <libdariadb/utils/fs.h>
#include <libclient/client.h>
#include <memory>
int main(int argc,char**argv){
	unsigned short server_port = 2001;
	std::string server_host = "127.0.0.1";
	dariadb::net::client::Client::Param p(server_host, server_port);
}

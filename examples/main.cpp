#include <libdariadb/engine.h>
#include <libdariadb/utils/fs.h>
#include <libclient/client.h>
#include <memory>
int main(int argc,char**argv){
	dariadb::storage::Options::start();
	dariadb::storage::Options::instance()->path = "example_storage";

	auto stor = std::make_unique<dariadb::storage::Engine>();

    dariadb::net::client::Client::Param p("127.0.0.1", 8080);
    dariadb::net::client::Client_Ptr client{ new dariadb::net::client::Client(p) };
}

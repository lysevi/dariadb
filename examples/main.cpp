#include <libdariadb/engine.h>
#include <libdariadb/utils/fs.h>
#include <memory>
int main(int argc,char**argv){
	dariadb::storage::Options::start();
	dariadb::storage::Options::instance()->path = "example_storage";

	auto stor = std::make_unique<dariadb::storage::Engine>();
}
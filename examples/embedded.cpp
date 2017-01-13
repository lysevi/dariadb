#include <libdariadb/engine.h>
#include <iostream>

int main(int argc,char**argv){

	auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings("exampledb") };
	settings->save();

	auto stor = std::make_unique<dariadb::storage::Engine>(settings);
	auto m = dariadb::Meas::empty(1);
	m.time = dariadb::timeutil::current_time();
	m.value = 0;
	auto status = stor->append(m);
	if (status.writed != 1) {
		//error;
		std::cerr << "Error: " << status.error_message << std::endl;;
	}
	else {
		std::cout << "succes" << std::endl;
	}
}

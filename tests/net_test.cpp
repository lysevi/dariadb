#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <thread>
#include <chrono>

typedef boost::shared_ptr<boost::asio::ip::tcp::socket> socket_ptr;

void client_session(socket_ptr sock)
{
	while (true)
	{
		char data[512];
		size_t len = sock->read_some(boost::asio::buffer(data));
		if (len > 0)
			write(*sock, boost::asio::buffer("ok", 2));
	}
}

void create_server()
{
	
	boost::asio::io_service service;

	boost::asio::ip::tcp::endpoint ep(boost::asio::ip::tcp::v4(), 2001); // listen on 2001
	boost::asio::ip::tcp::acceptor acc(service, ep);
	while (true)
	{
		socket_ptr sock(new boost::asio::ip::tcp::socket(service));
		acc.accept(*sock);
		boost::thread(boost::bind(client_session, sock));
	}
}

void connect_handler(const boost::system::error_code & ec)
{
	if (ec) {
		auto msg = ec.message();
		std::cout << "connect: " << msg << std::endl;
	}
	else {
		std::cout << "connect success "<< std::endl;
	}
}

void create_client()
{
	boost::asio::io_service service;
	boost::asio::ip::tcp::endpoint ep(boost::asio::ip::address::from_string("127.0.0.1"), 2001);
	boost::asio::ip::tcp::socket sock(service);
	sock.async_connect(ep, connect_handler);
	service.run();
}

BOOST_AUTO_TEST_CASE(Empty) {
	std::thread t1(create_server);
    std::this_thread::sleep_for(std::chrono::seconds(1));
	std::thread t2(create_client);

	t1.join();
	t2.join();
}

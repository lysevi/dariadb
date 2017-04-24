#include <libdariadb/utils/logger.h>
#include <gtest/gtest.h>
#include <iostream>
#include <list>

using ::testing::EmptyTestEventListener;
using ::testing::InitGoogleTest;
using ::testing::Test;
using ::testing::TestCase;
using ::testing::TestEventListeners;
using ::testing::TestInfo;
using ::testing::TestPartResult;
using ::testing::UnitTest;

#if WIN32
#if DEBUG
#define ENABLE_MEMORY_LEAK_DETECTION
#endif
#endif

class UnitTestLogger : public dariadb::utils::ILogger {
public:
  UnitTestLogger() {}
  ~UnitTestLogger() {}
  void message(dariadb::utils::LOG_MESSAGE_KIND kind, const std::string &msg) {
    std::stringstream ss;
    switch (kind) {
    case dariadb::utils::LOG_MESSAGE_KIND::FATAL:
      ss << "[err] " << msg << std::endl;
      break;
    case dariadb::utils::LOG_MESSAGE_KIND::INFO:
      ss << "[inf] " << msg << std::endl;
      break;
    case dariadb::utils::LOG_MESSAGE_KIND::MESSAGE:
      ss << "[dbg] " << msg << std::endl;
      break;
    }
	//std::cout << ss.str();
    _messages.push_back(ss.str());
  }

  void dump_all() {
    for (auto m : _messages) {
      std::cerr << m;
    }
  }

private:
  std::list<std::string> _messages;
};

class LoggerControl : public EmptyTestEventListener {
private:
  virtual void OnTestStart(const TestInfo & /* test_info */) {
    _raw_ptr = new UnitTestLogger();
    _logger = dariadb::utils::ILogger_ptr{_raw_ptr};
    dariadb::utils::LogManager::start(_logger);
  }

  virtual void OnTestEnd(const TestInfo &test_info) {
    if (test_info.result()->Failed()) {
      _raw_ptr->dump_all();
    }
    dariadb::utils::LogManager::stop();
    _logger = nullptr;
  }
  UnitTestLogger *_raw_ptr;
  dariadb::utils::ILogger_ptr _logger;
};

#ifdef ENABLE_MEMORY_LEAK_DETECTION
#define _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#include <stdlib.h>

class LeakChecker : public EmptyTestEventListener {
private:
  virtual void OnTestStart(const TestInfo &) { _CrtMemCheckpoint(&s1); }

  virtual void OnTestEnd(const TestInfo &) {
    _CrtMemCheckpoint(&s2);
    if (_CrtMemDifference(&s3, &s1, &s2)) {
      _CrtMemDumpAllObjectsSince(&s3);
    }
  }

  _CrtMemState s1 = {0}, s2 = {0}, s3 = {0};
};
#endif

//TEST(Logger, FailedTest) {
//  dariadb::logger_info(1);
//  dariadb::logger_info(2);
//  dariadb::logger_info(3);
//  throw std::logic_error("error");
//  EXPECT_TRUE(true);
//
//  dariadb::logger_info(4);
//}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  TestEventListeners &listeners = UnitTest::GetInstance()->listeners();
#ifdef ENABLE_MEMORY_LEAK_DETECTION
/* _CrtSetReportMode(_CRT_WARN, _CRTDBG_MODE_FILE);
 _CrtSetReportFile(_CRT_WARN, _CRTDBG_FILE_STDOUT);
 _CrtSetReportMode(_CRT_ERROR, _CRTDBG_MODE_FILE);
 _CrtSetReportFile(_CRT_ERROR, _CRTDBG_FILE_STDOUT);
 _CrtSetReportMode(_CRT_ASSERT, _CRTDBG_MODE_FILE);
 _CrtSetReportFile(_CRT_ASSERT, _CRTDBG_FILE_STDOUT);
 TestEventListeners &listeners = UnitTest::GetInstance()->listeners();
 listeners.Append(new LeakChecker);*/
#endif

  listeners.Append(new LoggerControl);
  return RUN_ALL_TESTS();
}
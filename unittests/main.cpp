#include <gtest/gtest.h>

#include <iostream>

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

#ifdef ENABLE_MEMORY_LEAK_DETECTION
#define _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#include <stdlib.h>

class LeakChecker : public EmptyTestEventListener {
private:
  virtual void OnTestStart(const TestInfo & /* test_info */) { _CrtMemCheckpoint(&s1); }

  virtual void OnTestEnd(const TestInfo & /* test_info */) {
    _CrtMemCheckpoint(&s2);
    if (_CrtMemDifference(&s3, &s1, &s2)) {
      _CrtMemDumpAllObjectsSince(&s3);
    }
  }

  _CrtMemState s1 = {0}, s2 = {0}, s3 = {0};
};
#endif
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef ENABLE_MEMORY_LEAK_DETECTION
  _CrtSetReportMode(_CRT_WARN, _CRTDBG_MODE_FILE);
  _CrtSetReportFile(_CRT_WARN, _CRTDBG_FILE_STDOUT);
  _CrtSetReportMode(_CRT_ERROR, _CRTDBG_MODE_FILE);
  _CrtSetReportFile(_CRT_ERROR, _CRTDBG_FILE_STDOUT);
  _CrtSetReportMode(_CRT_ASSERT, _CRTDBG_MODE_FILE);
  _CrtSetReportFile(_CRT_ASSERT, _CRTDBG_FILE_STDOUT);
  TestEventListeners &listeners = UnitTest::GetInstance()->listeners();
  listeners.Append(new LeakChecker);
#endif
  return RUN_ALL_TESTS();
}
#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <libdariadb/compression/v2/bytebuffer.h>
#include <libdariadb/compression/v2/delta.h>
#include <libdariadb/compression/compression.h>
#include <libdariadb/compression/delta.h>
#include <libdariadb/compression/flag.h>
#include <libdariadb/compression/id.h>
#include <libdariadb/compression/xor.h>
#include <libdariadb/timeutil.h>

#include <chrono>
#include <iterator>
#include <sstream>
#include <algorithm>

using dariadb::compression::v2::ByteBuffer;
using dariadb::compression::v2::ByteBuffer_Ptr;
using dariadb::compression::BinaryBuffer;
using dariadb::compression::BinaryBuffer_Ptr;

class Testable_DeltaCompressor : public dariadb::compression::DeltaCompressor {
public:
  Testable_DeltaCompressor(const BinaryBuffer_Ptr &buf)
      : dariadb::compression::DeltaCompressor(buf) {}

  uint64_t get_prev_delta() const { return this->_prev_delta; }
  dariadb::Time get_prev_time() const { return this->_prev_time; }
  dariadb::Time get_first() const { return this->_first; }
  BinaryBuffer_Ptr get_bw() const { return this->_bw; }
  bool is_first() const { return this->_is_first; }
};

class Testable_DeltaDeCompressor : public dariadb::compression::DeltaDeCompressor {
public:
  Testable_DeltaDeCompressor(const BinaryBuffer_Ptr &buf, dariadb::Time first)
      : dariadb::compression::DeltaDeCompressor(buf, first) {}

  uint64_t get_prev_delta() const { return this->_prev_delta; }
  dariadb::Time get_prev_time() const { return this->_prev_time; }
  BinaryBuffer_Ptr get_bw() const { return this->_bw; }
};

class Testable_XorCompressor : public dariadb::compression::XorCompressor {
public:
  Testable_XorCompressor(const BinaryBuffer_Ptr &buf)
      : dariadb::compression::XorCompressor(buf) {}

  BinaryBuffer_Ptr get_bw() const { return this->_bw; }
  dariadb::Value get_prev_value() const {
    return dariadb::compression::inner::flat_int_to_double(this->_prev_value);
  }
  dariadb::Value get_first() const {
    return dariadb::compression::inner::flat_int_to_double(this->_first);
  }
  bool is_first() const { return this->_is_first; }
  void set_is_first(bool flag) { this->_is_first = flag; }
  uint8_t get_prev_lead() const { return _prev_lead; }
  uint8_t get_prev_tail() const { return _prev_tail; }
  void set_prev_lead(uint8_t v) { _prev_lead = v; }
  void set_prev_tail(uint8_t v) { _prev_tail = v; }
};

BOOST_AUTO_TEST_CASE(binary_writer) {
  const size_t buffer_size = 10;
  const size_t writed_bits = 7 * buffer_size;
  uint8_t buffer[buffer_size];
  // check ctor
  BinaryBuffer b({std::begin(buffer), std::end(buffer)});
  BOOST_CHECK_EQUAL(b.cap(), buffer_size);

  BOOST_CHECK_EQUAL(b.bitnum(), 7);
  BOOST_CHECK_EQUAL(b.pos(), size_t(buffer_size - 1));

  // check incs work fine
  b.incbit();
  BOOST_CHECK_EQUAL(b.bitnum(), 6);
  BOOST_CHECK_EQUAL(b.pos(), size_t(buffer_size - 1));

  b.incbit();
  b.incbit();
  b.incbit();
  b.incbit();
  b.incbit();
  b.incbit();
  b.incbit();
  BOOST_CHECK_EQUAL(b.bitnum(), 7);
  BOOST_CHECK_EQUAL(b.pos(), size_t(buffer_size - 2));

  { // ctors test.
    BinaryBuffer copy_b(b);
    BOOST_CHECK_EQUAL(b.bitnum(), copy_b.bitnum());
    BOOST_CHECK_EQUAL(b.pos(), copy_b.pos());
    auto move_b = std::move(copy_b);
    BOOST_CHECK(size_t(copy_b.bitnum()) == copy_b.pos() &&
                size_t(copy_b.bitnum()) == copy_b.cap());
    BOOST_CHECK_EQUAL(copy_b.bitnum(), 0);

    BOOST_CHECK_EQUAL(move_b.bitnum(), b.bitnum());
    BOOST_CHECK_EQUAL(move_b.pos(), b.pos());
    BOOST_CHECK_EQUAL(move_b.cap(), b.cap());
  }
  // set/clr bit
  b.reset_pos();
  // write 101010101...
  for (size_t i = 0; i < writed_bits; i++) {
    if (i % 2) {
      b.setbit().incbit();
    } else {
      b.clrbit().incbit();
    }
  }

  b.reset_pos();

  for (size_t i = 0; i < writed_bits; i++) {
    if (i % 2) {
      BOOST_CHECK_EQUAL(b.getbit(), uint8_t(1));
    } else {
      BOOST_CHECK_EQUAL(b.getbit(), uint8_t(0));
    }
    b.incbit();
  }

  std::stringstream ss{};
  ss << b;
  BOOST_CHECK(ss.str().size() != 0);

  b.reset_pos();
  // clear all bits
  for (size_t i = 0; i < writed_bits; i++) {
    b.clrbit();
    BOOST_CHECK_EQUAL(b.getbit(), uint8_t(0));
    b.incbit();
  }

  {
    b.reset_pos();
    auto writed = std::numeric_limits<uint16_t>::max();
    b.write(writed, 15);
    b.reset_pos();
    auto readed = b.read(15);
    BOOST_CHECK_EQUAL(writed, readed);
  }
  {
    b.reset_pos();
    auto writed = std::numeric_limits<uint32_t>::max();
    b.write((uint64_t)writed, 31);
    b.reset_pos();
    auto readed = b.read(31);
    BOOST_CHECK_EQUAL(writed, readed);
  }

  { // max uint64
    b.reset_pos();
    auto writed = std::numeric_limits<uint64_t>::max();
    b.write((uint64_t)writed, 63);
    b.reset_pos();
    auto readed = b.read(63);
    BOOST_CHECK_EQUAL(writed, readed);
  }

  { // 0
    std::fill(std::begin(buffer), std::end(buffer), 0);
    b.reset_pos();
    uint64_t writed = 0;
    b.write((uint16_t)writed, 5);
    b.reset_pos();
    auto readed = b.read(5);
    BOOST_CHECK_EQUAL(writed, readed);
  }
}

BOOST_AUTO_TEST_CASE(DeltaCompressor_deltas) {
  using dariadb::compression::DeltaCompressor;
  BOOST_CHECK_EQUAL(DeltaCompressor::get_delta_64(1), 513);
  BOOST_CHECK_EQUAL(DeltaCompressor::get_delta_64(64), 576);
  BOOST_CHECK_EQUAL(DeltaCompressor::get_delta_64(63), 575);

  BOOST_CHECK_EQUAL(DeltaCompressor::get_delta_256(256), 3328);
  BOOST_CHECK_EQUAL(DeltaCompressor::get_delta_256(255), 3327);
  BOOST_CHECK_EQUAL(DeltaCompressor::get_delta_256(65), 3137);

  BOOST_CHECK_EQUAL(DeltaCompressor::get_delta_2048(2048), 59392);
  BOOST_CHECK_EQUAL(DeltaCompressor::get_delta_2048(257), 57601);
  BOOST_CHECK_EQUAL(DeltaCompressor::get_delta_2048(1500), 58844);

  BOOST_CHECK_EQUAL(DeltaCompressor::get_delta_big(2049), uint64_t(64424511489));
  BOOST_CHECK_EQUAL(DeltaCompressor::get_delta_big(65535), uint64_t(64424574975));
  BOOST_CHECK_EQUAL(DeltaCompressor::get_delta_big(4095), uint64_t(64424513535));
  BOOST_CHECK_EQUAL(DeltaCompressor::get_delta_big(4294967295), uint64_t(68719476735));
}

BOOST_AUTO_TEST_CASE(DeltaCompressor) {
  const size_t test_buffer_size = 100;

  const dariadb::Time t1 = 100;
  const dariadb::Time t2 = 150;
  const dariadb::Time t3 = 200;
  const dariadb::Time t4 = 2000;
  const dariadb::Time t5 = 3000;

  uint8_t buffer[test_buffer_size];
  std::fill(std::begin(buffer), std::end(buffer), 0);
  dariadb::utils::Range rng{std::begin(buffer), std::end(buffer)};
  auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);

  {
    Testable_DeltaCompressor dc{bw};
    BOOST_CHECK(dc.is_first());

    dc.append(t1);
    BOOST_CHECK(!dc.is_first());
    BOOST_CHECK_EQUAL(dc.get_first(), t1);
    BOOST_CHECK_EQUAL(dc.get_prev_time(), t1);

    dc.append(t1);
    BOOST_CHECK_EQUAL(dc.get_bw()->bitnum(), dariadb::compression::max_bit_pos - 1);
    BOOST_CHECK_EQUAL(buffer[0], 0);
  }
  {
    std::fill(std::begin(buffer), std::end(buffer), 0);
    bw->reset_pos();
    Testable_DeltaCompressor dc{bw};

    dc.append(t1);
    dc.append(t2);
    BOOST_CHECK_EQUAL(dc.get_prev_time(), t2);
    BOOST_CHECK_EQUAL(dc.get_prev_delta(), t2 - t1);
    BOOST_CHECK_EQUAL(int(buffer[test_buffer_size - 1]), int(140));
    BOOST_CHECK_EQUAL(int(buffer[test_buffer_size - 2]), int(128));
  }
  {
    std::fill(std::begin(buffer), std::end(buffer), 0);
    bw->reset_pos();
    Testable_DeltaCompressor dc{bw};

    dc.append(t1);
    dc.append(t3);
    BOOST_CHECK_EQUAL(dc.get_prev_time(), t3);
    BOOST_CHECK_EQUAL(dc.get_prev_delta(), t3 - t1);
    BOOST_CHECK_EQUAL(int(buffer[test_buffer_size - 1]), int(198));
    BOOST_CHECK_EQUAL(int(buffer[test_buffer_size - 2]), int(64));
  }

  {
    std::fill(std::begin(buffer), std::end(buffer), 0);
    bw->reset_pos();
    Testable_DeltaCompressor dc{bw};

    dc.append(t1);
    dc.append(t4);
    BOOST_CHECK_EQUAL(dc.get_prev_time(), t4);
    BOOST_CHECK_EQUAL(dc.get_prev_delta(), t4 - t1);
    BOOST_CHECK_EQUAL(int(buffer[test_buffer_size - 1]), int(231));
    BOOST_CHECK_EQUAL(int(buffer[test_buffer_size - 2]), int(108));
  }
  {
    std::fill(std::begin(buffer), std::end(buffer), 0);
    bw->reset_pos();
    Testable_DeltaCompressor dc{bw};

    dc.append(t1);
    dc.append(t5);
    BOOST_CHECK_EQUAL(dc.get_prev_time(), t5);
    BOOST_CHECK_EQUAL(dc.get_prev_delta(), t5 - t1);
    BOOST_CHECK_EQUAL(int(buffer[test_buffer_size - 1]), int(240));
    BOOST_CHECK_EQUAL(int(buffer[test_buffer_size - 2]), int(0));
    BOOST_CHECK_EQUAL(int(buffer[test_buffer_size - 3]), int(0));
    BOOST_CHECK_EQUAL(int(buffer[test_buffer_size - 4]), int(181));
    BOOST_CHECK_EQUAL(int(buffer[test_buffer_size - 5]), int(64));
  }
}

BOOST_AUTO_TEST_CASE(DeltaDeCompressor) {
  const size_t test_buffer_size = 1000;
  const dariadb::Time t1 = 100;
  const dariadb::Time t2 = 150;
  const dariadb::Time t3 = 200;
  const dariadb::Time t4 = t3 + 100;
  const dariadb::Time t5 = t4 + 1000;
  const dariadb::Time t6 = t5 * 2;
  const dariadb::Time t7 = t6 - 50;

  uint8_t buffer[test_buffer_size];
  std::fill(std::begin(buffer), std::end(buffer), 0);
  dariadb::utils::Range rng{std::begin(buffer), std::end(buffer)};
  auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);
  {
    Testable_DeltaCompressor co(bw);

    co.append(t1);
    co.append(t2);
    co.append(t3);
    co.append(t4);
    co.append(t5);
    co.append(t6);
    co.append(t7);
    bw->reset_pos();
    Testable_DeltaDeCompressor dc(bw, t1);
    BOOST_CHECK_EQUAL(dc.read(), t2);
    BOOST_CHECK_EQUAL(dc.read(), t3);
    BOOST_CHECK_EQUAL(dc.read(), t4);
    BOOST_CHECK_EQUAL(dc.read(), t5);
    BOOST_CHECK_EQUAL(dc.read(), t6);
    BOOST_CHECK_EQUAL(dc.read(), t7);
  }

  std::fill(std::begin(buffer), std::end(buffer), 0);
  bw->reset_pos();
  {
    Testable_DeltaCompressor co(bw);
    dariadb::Time delta = 1;
    std::list<dariadb::Time> times{};
    const int steps = 30;
    for (int i = 0; i < steps; i++) {
      co.append(delta);
      times.push_back(delta);
      delta *= 2;
    }
    bw->reset_pos();
    Testable_DeltaDeCompressor dc(bw, times.front());
    times.pop_front();
    for (auto &t : times) {
      auto readed = dc.read();
      BOOST_CHECK_EQUAL(readed, t);
    }
  }
  std::fill(std::begin(buffer), std::end(buffer), 0);
  bw->reset_pos();
  { // decrease
    Testable_DeltaCompressor co(bw);
    std::vector<dariadb::Time> deltas{50, 255, 1024, 2050};
    dariadb::Time delta = 1000000;
    std::list<dariadb::Time> times{};
    const int steps = 50;
    for (int i = 0; i < steps; i++) {
      co.append(delta);
      times.push_back(delta);
      delta -= deltas[i % deltas.size()];
    }
    bw->reset_pos();
    Testable_DeltaDeCompressor dc(bw, times.front());
    times.pop_front();
    for (auto &t : times) {
      auto readed = dc.read();
      BOOST_CHECK_EQUAL(readed, t);
    }
  }

  std::fill(std::begin(buffer), std::end(buffer), 0);
  bw->reset_pos();
  { // 333,0,0,0
    Testable_DeltaCompressor co(bw);
    dariadb::Time delta = 333;
    std::list<dariadb::Time> times{};
    for (int i = 0; i < 10; i++) {
      co.append(delta);
      times.push_back(delta);
      delta = 0;
    }
    bw->reset_pos();
    Testable_DeltaDeCompressor dc(bw, times.front());
    times.pop_front();
    for (auto &t : times) {
      auto readed = dc.read();
      BOOST_CHECK_EQUAL(readed, t);
    }
  }
}

BOOST_AUTO_TEST_CASE(flat_converters) {
  double pi = 3.14;
  auto ival = dariadb::compression::inner::flat_double_to_int(pi);
  auto dval = dariadb::compression::inner::flat_int_to_double(ival);
  BOOST_CHECK_CLOSE(dval, pi, 0.0001);
}

BOOST_AUTO_TEST_CASE(XorCompressor) {
  const size_t test_buffer_size = 1000;

  const dariadb::Value t1 = 240;
  // const dariadb::Value t2=224;

  uint8_t buffer[test_buffer_size];
  std::fill(std::begin(buffer), std::end(buffer), 0);
  dariadb::utils::Range rng{std::begin(buffer), std::end(buffer)};
  auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);
  {
    Testable_XorCompressor dc(bw);
    BOOST_CHECK(dc.is_first());

    dc.append(t1);
    BOOST_CHECK(!dc.is_first());
    BOOST_CHECK_EQUAL(dc.get_first(), t1);
    BOOST_CHECK_EQUAL(dc.get_prev_value(), t1);
  }
  using dariadb::compression::XorDeCompressor;
  std::fill(std::begin(buffer), std::end(buffer), 0);
  bw->reset_pos();
  { // cur==prev
    Testable_XorCompressor co(bw);

    auto v1 = dariadb::Value(240);
    auto v2 = dariadb::Value(240);
    co.append(v1);
    co.append(v2);

    XorDeCompressor dc(bw, t1);
    BOOST_CHECK_EQUAL(dc.read(), v2);
  }

  std::fill(std::begin(buffer), std::end(buffer), 0);
  bw->reset_pos();
  { // cur!=prev
    Testable_XorCompressor co(bw);

    auto v1 = dariadb::Value(240);
    auto v2 = dariadb::Value(96);
    auto v3 = dariadb::Value(176);
    co.append(v1);
    co.append(v2);
    co.append(v3);

    bw->reset_pos();
    XorDeCompressor dc(bw, v1);
    BOOST_CHECK_EQUAL(dc.read(), v2);
    BOOST_CHECK_EQUAL(dc.read(), v3);
  }
  std::fill(std::begin(buffer), std::end(buffer), 0);
  bw->reset_pos();
  { // tail/lead is equals
    Testable_XorCompressor co{bw};

    auto v1 = dariadb::Value(3840);
    auto v2 = dariadb::Value(3356);
    co.append(v1);
    co.append(v2);

    bw->reset_pos();
    XorDeCompressor dc(bw, v1);
    BOOST_CHECK_EQUAL(dc.read(), v2);
  }
  std::fill(std::begin(buffer), std::end(buffer), 0);
  bw->reset_pos();
  { // tail/lead not equals
    Testable_XorCompressor co{bw};

    auto v1 = dariadb::Value(3840);
    auto v2 = dariadb::Value(3328);
    co.append(v1);
    co.append(v2);

    bw->reset_pos();
    XorDeCompressor dc(bw, v1);
    BOOST_CHECK_EQUAL(dc.read(), v2);
  }
  std::fill(std::begin(buffer), std::end(buffer), 0);
  bw->reset_pos();
  {
    Testable_XorCompressor co(bw);

    std::list<dariadb::Value> values{};
    dariadb::Value delta = 1;

    for (int i = 0; i < 100; i++) {
      co.append(delta);
      values.push_back(delta);
      delta += 1;
    }

    bw->reset_pos();
    XorDeCompressor dc(bw, values.front());
    values.pop_front();
    for (auto &v : values) {
      BOOST_CHECK_EQUAL(dc.read(), v);
    }
  }
  std::fill(std::begin(buffer), std::end(buffer), 0);
  bw->reset_pos();
  { // 333,0,0,0,0,0
    Testable_XorCompressor co(bw);
    std::list<dariadb::Value> values{};
    dariadb::Value delta = dariadb::Value(333);

    for (int i = 0; i < 10; i++) {
      co.append(delta);
      values.push_back(delta);
      delta = 0;
      ;
    }

    bw->reset_pos();
    XorDeCompressor dc(bw, values.front());
    values.pop_front();
    for (auto &v : values) {
      auto readed_value = dc.read();
      BOOST_CHECK_EQUAL(readed_value, v);
    }
  }
  bw->reset_pos();
  { // maxValue
    std::fill(rng.begin, rng.end, 0);
    Testable_XorCompressor co(bw);
    auto max_value = std::numeric_limits<dariadb::Value>::max();
    auto v1 = dariadb::Value(0);
    auto v2 = dariadb::Value(max_value);

    co.append(v1);
    co.append(v2);

    bw->reset_pos();
    XorDeCompressor dc(bw, 0);
    BOOST_CHECK_EQUAL(dc.read(), v2);
  }
}

BOOST_AUTO_TEST_CASE(FlagCompressor) {
  { // 1,2,3,4,5...
    const size_t test_buffer_size = 1000;
    uint8_t buffer[test_buffer_size];
    std::fill(std::begin(buffer), std::end(buffer), 0);
    using dariadb::compression::FlagCompressor;
    using dariadb::compression::FlagDeCompressor;

    dariadb::utils::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);
    FlagCompressor fc(bw);

    std::list<dariadb::Flag> flags{};
    dariadb::Flag delta = 1;
    for (int i = 0; i < 10; i++) {
      fc.append(delta);
      flags.push_back(delta);
      delta++;
    }
    bw->reset_pos();
    FlagDeCompressor fd(bw, flags.front());
    flags.pop_front();
    for (auto f : flags) {
      auto v = fd.read();
      BOOST_CHECK_EQUAL(v, f);
    }
  }
  { // 333,0,0,0,0...
    const size_t test_buffer_size = 100;
    uint8_t buffer[test_buffer_size];
    std::fill(std::begin(buffer), std::end(buffer), 0);
    using dariadb::compression::FlagCompressor;
    using dariadb::compression::FlagDeCompressor;

    dariadb::utils::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);
    FlagCompressor fc(bw);

    std::list<dariadb::Flag> flags{};
    dariadb::Flag delta = 333;
    fc.append(delta);
    flags.push_back(delta);
    delta = dariadb::Flag(0);
    for (int i = 0; i < 10; i++) {
      fc.append(delta);
      flags.push_back(delta);
    }
    bw->reset_pos();
    FlagDeCompressor fd(bw, flags.front());
    flags.pop_front();
    for (auto f : flags) {
      auto v = fd.read();
      BOOST_CHECK_EQUAL(v, f);
    }
  }
}

BOOST_AUTO_TEST_CASE(IdCompressor) {
  { // 1,2,3,4,5...
    const size_t test_buffer_size = 1000;
    uint8_t buffer[test_buffer_size];
    std::fill(std::begin(buffer), std::end(buffer), 0);
    using dariadb::compression::IdCompressor;
    using dariadb::compression::IdDeCompressor;

    dariadb::utils::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);
    IdCompressor fc(bw);

    std::list<dariadb::Id> ids{};
    dariadb::Id delta = 1;
    for (int i = 0; i < 10; i++) {
      fc.append(delta);
      ids.push_back(delta);
      delta += 1000;
    }
    bw->reset_pos();
    IdDeCompressor fd(bw, ids.front());
    ids.pop_front();
    for (auto f : ids) {
      auto v = fd.read();
      BOOST_CHECK_EQUAL(v, f);
    }
  }
}

BOOST_AUTO_TEST_CASE(CompressedBlock) {
  const size_t test_buffer_size = 128;

  uint8_t b_begin[test_buffer_size];
  auto b_end = std::end(b_begin);

  std::fill(b_begin, b_end, 0);
  dariadb::utils::Range rng{b_begin, b_end};

  using dariadb::compression::CopmressedWriter;
  using dariadb::compression::CopmressedReader;

  auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);

  CopmressedWriter cwr(bw);

  std::list<dariadb::Meas> meases{};
  auto zer_t = dariadb::timeutil::current_time();
  for (int i = 0;; i++) {
    auto m = dariadb::Meas::empty(1);
    m.time = zer_t++;
    m.flag = i;
    m.src = i;
    m.value = i;
    if (!cwr.append(m)) {
      BOOST_CHECK(cwr.is_full());
      break;
    }
    meases.push_back(m);
  }
  BOOST_CHECK_LT(cwr.used_space(), test_buffer_size);

  bw->reset_pos();
  CopmressedReader crr(bw, meases.front());

  meases.pop_front();
  for (auto &m : meases) {
    auto r_m = crr.read();
    BOOST_CHECK(m == r_m);
  }
}

BOOST_AUTO_TEST_CASE(CompressedBlockZeroValues) {
  const size_t test_buffer_size = 128;

  uint8_t b_begin[test_buffer_size];
  auto b_end = std::end(b_begin);

  std::fill(b_begin, b_end, 0);
  dariadb::utils::Range rng{b_begin, b_end};

  using dariadb::compression::CopmressedWriter;
  using dariadb::compression::CopmressedReader;

  auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);

  CopmressedWriter cwr(bw);

  std::list<dariadb::Meas> meases{};

  auto m = dariadb::Meas::empty();
  m.time = 111;
  m.value = 222;
  m.src = m.flag = 333;
  cwr.append(m);
  meases.push_back(m);

  for (int i = 0;; i++) {
    m.time = 0;
    m.flag = 0;
    m.value = 0;
    m.src = 0;
    if (!cwr.append(m)) {
      break;
    }
    meases.push_back(m);
  }

  bw->reset_pos();
  CopmressedReader crr(bw, meases.front());

  meases.pop_front();
  for (auto &wm : meases) {
    auto r_m = crr.read();
    BOOST_CHECK(wm == r_m);
  }
}

BOOST_AUTO_TEST_CASE(ByteBufferTest) {
	const size_t buffer_size = 256;
	uint8_t buffer[buffer_size];
	std::fill_n(buffer, buffer_size, uint8_t(0));
	{
		ByteBuffer b({ std::begin(buffer), std::end(buffer) });
		b.write(std::numeric_limits<int64_t>::min());
		b.write(std::numeric_limits<int64_t>::max());

		b.write(std::numeric_limits<uint64_t>::min());
		b.write(std::numeric_limits<uint64_t>::max());

		b.write(std::numeric_limits<int32_t>::min());
		b.write(std::numeric_limits<int32_t>::max());

		b.write(std::numeric_limits<uint32_t>::min());
		b.write(std::numeric_limits<uint32_t>::max());

		b.write(std::numeric_limits<int16_t>::min());
		b.write(std::numeric_limits<int16_t>::max());

		b.write(std::numeric_limits<uint16_t>::min());
		b.write(std::numeric_limits<uint16_t>::max());

		b.write(std::numeric_limits<int8_t>::min());
		b.write(std::numeric_limits<int8_t>::max());

		b.write(std::numeric_limits<uint8_t>::min());
		b.write(std::numeric_limits<uint8_t>::max());
	}
	{
		ByteBuffer b({ std::begin(buffer), std::end(buffer) });
		
		auto i64=b.read<int64_t>();
		BOOST_CHECK_EQUAL(i64, std::numeric_limits<int64_t>::min());
		i64 = b.read<int64_t>();
		BOOST_CHECK_EQUAL(i64, std::numeric_limits<int64_t>::max());

		auto ui64 = b.read<uint64_t>();
		BOOST_CHECK_EQUAL(ui64, std::numeric_limits<uint64_t>::min());
		ui64 = b.read<uint64_t>();
		BOOST_CHECK_EQUAL(ui64, std::numeric_limits<uint64_t>::max());

		auto i32 = b.read<int32_t>();
		BOOST_CHECK_EQUAL(i32, std::numeric_limits<int32_t>::min());
		i32 = b.read<int32_t>();
		BOOST_CHECK_EQUAL(i32, std::numeric_limits<int32_t>::max());

		auto ui32 = b.read<uint32_t>();
		BOOST_CHECK_EQUAL(ui32, std::numeric_limits<uint32_t>::min());
		ui32 = b.read<uint32_t>();
		BOOST_CHECK_EQUAL(ui32, std::numeric_limits<uint32_t>::max());

		auto i16 = b.read<int16_t>();
		BOOST_CHECK_EQUAL(i16, std::numeric_limits<int16_t>::min());
		i16 = b.read<int16_t>();
		BOOST_CHECK_EQUAL(i16, std::numeric_limits<int16_t>::max());

		auto ui16 = b.read<uint16_t>();
		BOOST_CHECK_EQUAL(ui16, std::numeric_limits<uint16_t>::min());
		ui16 = b.read<uint16_t>();
		BOOST_CHECK_EQUAL(ui16, std::numeric_limits<uint16_t>::max());

		auto i8 = b.read<int8_t>();
		BOOST_CHECK_EQUAL(i8, std::numeric_limits<int8_t>::min());
		i8 = b.read<int8_t>();
		BOOST_CHECK_EQUAL(i8, std::numeric_limits<int8_t>::max());

		auto ui8 = b.read<uint8_t>();
		BOOST_CHECK_EQUAL(ui8, std::numeric_limits<uint8_t>::min());
		ui8 = b.read<uint8_t>();
		BOOST_CHECK_EQUAL(ui8, std::numeric_limits<uint8_t>::max());
	}
}

BOOST_AUTO_TEST_CASE(DeltaDeltaTest) {
	const size_t test_buffer_size = 100;

	const dariadb::Time t1 = 100;
	const dariadb::Time t2 = 150;
	const dariadb::Time t3 = 2000;
	const dariadb::Time t4 = 1048175;
	const dariadb::Time t5 = std::numeric_limits<uint32_t>::max();

	uint8_t buffer[test_buffer_size];
	{// on t1-t0 < 1byte;
		std::fill_n(buffer, test_buffer_size, 0);
		dariadb::utils::Range rng{ std::begin(buffer), std::end(buffer) };
		auto bw = std::make_shared<dariadb::compression::v2::ByteBuffer>(rng);

		dariadb::compression::v2::DeltaCompressor dc{ bw };
		BOOST_CHECK(dc.is_first);

		dc.append(t1);
		dc.append(t2);
	}
	{//decompression
		dariadb::utils::Range rng{ std::begin(buffer), std::end(buffer) };
		auto bw_d = std::make_shared<dariadb::compression::v2::ByteBuffer>(rng);
		dariadb::compression::v2::DeltaDeCompressor dc{ bw_d,t1 };

		auto readed2 = dc.read();
		BOOST_CHECK_EQUAL(readed2, t2);
	}


	{// on t1-t0 < 2bytes;
		std::fill_n(buffer, test_buffer_size, 0);
		dariadb::utils::Range rng{ std::begin(buffer), std::end(buffer) };
		auto bw = std::make_shared<dariadb::compression::v2::ByteBuffer>(rng);

		dariadb::compression::v2::DeltaCompressor dc{ bw };
		BOOST_CHECK(dc.is_first);

		dc.append(t1);
		dc.append(t3);
	}
	{//decompression
		dariadb::utils::Range rng{ std::begin(buffer), std::end(buffer) };
		auto bw_d = std::make_shared<dariadb::compression::v2::ByteBuffer>(rng);
		dariadb::compression::v2::DeltaDeCompressor dc{ bw_d,t1 };

		auto readed2 = dc.read();
		BOOST_CHECK_EQUAL(readed2, t3);
	}

	{// on t1-t0 < 3bytes;
		std::fill_n(buffer, test_buffer_size, 0);
		dariadb::utils::Range rng{ std::begin(buffer), std::end(buffer) };
		auto bw = std::make_shared<dariadb::compression::v2::ByteBuffer>(rng);

		dariadb::compression::v2::DeltaCompressor dc{ bw };
		BOOST_CHECK(dc.is_first);

		dc.append(t1);
		dc.append(t4);
	}
	{//decompression
		dariadb::utils::Range rng{ std::begin(buffer), std::end(buffer) };
		auto bw_d = std::make_shared<dariadb::compression::v2::ByteBuffer>(rng);
		dariadb::compression::v2::DeltaDeCompressor dc{ bw_d,t1 };

		auto readed2 = dc.read();
		BOOST_CHECK_EQUAL(readed2, t4);
	}


	{// on t1-t0 > 3bytes;
		std::fill_n(buffer, test_buffer_size, 0);
		dariadb::utils::Range rng{ std::begin(buffer), std::end(buffer) };
		auto bw = std::make_shared<dariadb::compression::v2::ByteBuffer>(rng);

		dariadb::compression::v2::DeltaCompressor dc{ bw };
		BOOST_CHECK(dc.is_first);

		dc.append(t1);
		dc.append(t5);
	}
	{//decompression
		dariadb::utils::Range rng{ std::begin(buffer), std::end(buffer) };
		auto bw_d = std::make_shared<dariadb::compression::v2::ByteBuffer>(rng);
		dariadb::compression::v2::DeltaDeCompressor dc{ bw_d,t1 };

		auto readed2 = dc.read();
		BOOST_CHECK_EQUAL(readed2, t5);
	}
}
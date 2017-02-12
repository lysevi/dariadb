#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <libdariadb/compression/bytebuffer.h>
#include <libdariadb/compression/compression.h>
#include <libdariadb/compression/delta.h>
#include <libdariadb/compression/flag.h>
#include <libdariadb/compression/xor.h>
#include <libdariadb/timeutil.h>

#include <algorithm>
#include <chrono>
#include <iterator>

using dariadb::compression::ByteBuffer;
using dariadb::compression::ByteBuffer_Ptr;

BOOST_AUTO_TEST_CASE(flat_converters) {
  double pi = 3.14;
  auto ival = dariadb::compression::inner::flat_double_to_int(pi);
  auto dval = dariadb::compression::inner::flat_int_to_double(ival);
  BOOST_CHECK_CLOSE(dval, pi, 0.0001);
}

BOOST_AUTO_TEST_CASE(ByteBufferTest) {
  const size_t buffer_size = 256;
  uint8_t buffer[buffer_size];
  std::fill_n(buffer, buffer_size, uint8_t(0));
  {
    ByteBuffer b({std::begin(buffer), std::end(buffer)});
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
    ByteBuffer b({std::begin(buffer), std::end(buffer)});

    auto i64 = b.read<int64_t>();
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

BOOST_AUTO_TEST_CASE(DeltaDeltav2Test) {
  const size_t test_buffer_size = 100;

  const dariadb::Time t0 = 0;
  const dariadb::Time t1 = 100;
  const dariadb::Time t2 = 130;
  const dariadb::Time t3 = 2000;
  const dariadb::Time t4 = 524285;
  const dariadb::Time t5 = std::numeric_limits<uint32_t>::max();

  uint8_t buffer[test_buffer_size];
  { // on t1-t0 < 1byte;
    std::fill_n(buffer, test_buffer_size, 0);
    dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);

    dariadb::compression::DeltaCompressor dc{bw};
    BOOST_CHECK(dc.is_first);

    dc.append(t1);
    dc.append(t2);
  }
  { // decompression
    dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw_d = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::DeltaDeCompressor dc{bw_d, t1};

    auto readed2 = dc.read();
    BOOST_CHECK_EQUAL(readed2, t2);
  }

  { // on t1-t0 < 2bytes;
    std::fill_n(buffer, test_buffer_size, 0);
    dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);

    dariadb::compression::DeltaCompressor dc{bw};
    BOOST_CHECK(dc.is_first);

    dc.append(t1);
    dc.append(t3);
  }
  { // decompression
    dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw_d = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::DeltaDeCompressor dc{bw_d, t1};

    auto readed2 = dc.read();
    BOOST_CHECK_EQUAL(readed2, t3);
  }

  { // on t1-t0 < 3bytes;
    std::fill_n(buffer, test_buffer_size, 0);
    dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);

    dariadb::compression::DeltaCompressor dc{bw};
    BOOST_CHECK(dc.is_first);

    dc.append(t1);
    dc.append(t4);
  }
  { // decompression
    dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw_d = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::DeltaDeCompressor dc{bw_d, t1};

    auto readed2 = dc.read();
    BOOST_CHECK_EQUAL(readed2, t4);
  }

  { // on t1-t0 > 3bytes;
    std::fill_n(buffer, test_buffer_size, 0);
    dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);

    dariadb::compression::DeltaCompressor dc{bw};
    BOOST_CHECK(dc.is_first);

    dc.append(t1);
    dc.append(t5);
  }
  { // decompression
    dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw_d = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::DeltaDeCompressor dc{bw_d, t1};

    auto readed2 = dc.read();
    BOOST_CHECK_EQUAL(readed2, t5);
  }

  { // compress all;
    std::fill_n(buffer, test_buffer_size, 0);
    dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);

    dariadb::compression::DeltaCompressor dc{bw};

    dc.append(t0);
    dc.append(t1);
    dc.append(t2);
    dc.append(t3);
    dc.append(t4);
    dc.append(t5);
  }

  { // decompress all
    dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw_d = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::DeltaDeCompressor dc{bw_d, t0};

    auto readed = dc.read();
    BOOST_CHECK_EQUAL(readed, t1);

    readed = dc.read();
    BOOST_CHECK_EQUAL(readed, t2);

    readed = dc.read();
    BOOST_CHECK_EQUAL(readed, t3);

    readed = dc.read();
    BOOST_CHECK_EQUAL(readed, t4);

    readed = dc.read();
    BOOST_CHECK_EQUAL(readed, t5);
  }

  { // compress all; backward
    std::fill_n(buffer, test_buffer_size, 0);
    dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);

    dariadb::compression::DeltaCompressor dc{bw};

    dc.append(t5);
    dc.append(t4);
    dc.append(t3);
    dc.append(t2);
    dc.append(t1);
    dc.append(t0);
  }

  { // decompress all
    dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw_d = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::DeltaDeCompressor dc{bw_d, t5};

    auto readed = dc.read();
    BOOST_CHECK_EQUAL(readed, t4);

    readed = dc.read();
    BOOST_CHECK_EQUAL(readed, t3);

    readed = dc.read();
    BOOST_CHECK_EQUAL(readed, t2);

    readed = dc.read();
    BOOST_CHECK_EQUAL(readed, t1);

    readed = dc.read();
    BOOST_CHECK_EQUAL(readed, t0);
  }

  { // compress all; big delta
    std::fill_n(buffer, test_buffer_size, 0);
    dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);

    dariadb::compression::DeltaCompressor dc{bw};

    dc.append(9999);
    dc.append(0);
  }

  { // decompress all
    dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw_d = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::DeltaDeCompressor dc{bw_d, 9999};

    auto readed = dc.read();
    BOOST_CHECK_EQUAL(readed, 0);
  }
}

BOOST_AUTO_TEST_CASE(XorCompressorV2Test) {
  const size_t test_buffer_size = 1000;

  const dariadb::Value t1 = 240;
  // const dariadb::Value t2=224;

  uint8_t buffer[test_buffer_size];
  std::fill(std::begin(buffer), std::end(buffer), 0);
  dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};

  {
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorCompressor dc(bw);
    BOOST_CHECK(dc._is_first);

    dc.append(t1);
    BOOST_CHECK(!dc._is_first);
    BOOST_CHECK_EQUAL(
        dariadb::compression::inner::flat_int_to_double(dc._first), t1);
    BOOST_CHECK_EQUAL(
        dariadb::compression::inner::flat_int_to_double(dc._prev_value), t1);
  }

  { // cur==prev
    std::fill(std::begin(buffer), std::end(buffer), 0);
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorCompressor co(bw);

    auto v1 = dariadb::Value(240);
    auto v2 = dariadb::Value(240);
    co.append(v1);
    co.append(v2);

    dariadb::compression::XorDeCompressor dc(bw, t1);
    BOOST_CHECK_EQUAL(dc.read(), v2);
  }

  { // cur!=prev
    std::fill(std::begin(buffer), std::end(buffer), 0);
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorCompressor co(bw);

    auto v1 = dariadb::Value(240);
    auto v2 = dariadb::Value(96);
    auto v3 = dariadb::Value(176);
    co.append(v1);
    co.append(v2);
    co.append(v3);

    auto read_bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorDeCompressor dc(read_bw, v1);
    BOOST_CHECK_EQUAL(dc.read(), v2);
    BOOST_CHECK_EQUAL(dc.read(), v3);
  }
  std::fill(std::begin(buffer), std::end(buffer), 0);
  { // tail/lead is equals
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorCompressor co(bw);

    auto v1 = dariadb::Value(3840);
    auto v2 = dariadb::Value(3356);
    co.append(v1);
    co.append(v2);

    auto read_bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorDeCompressor dc(read_bw, v1);
    BOOST_CHECK_EQUAL(dc.read(), v2);
  }
  std::fill(std::begin(buffer), std::end(buffer), 0);
  { // tail/lead not equals
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorCompressor co(bw);

    auto v1 = dariadb::Value(3840);
    auto v2 = dariadb::Value(3328);
    co.append(v1);
    co.append(v2);

    auto read_bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorDeCompressor dc(read_bw, v1);
    BOOST_CHECK_EQUAL(dc.read(), v2);
  }
  std::fill(std::begin(buffer), std::end(buffer), 0);
  {
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorCompressor co(bw);

    std::list<dariadb::Value> values{};
    dariadb::Value delta = 1;

    for (int i = 0; i < 100; i++) {
      co.append(delta);
      values.push_back(delta);
      delta += 1;
    }

    auto read_bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorDeCompressor dc(read_bw, values.front());
    values.pop_front();
    for (auto &v : values) {
      BOOST_CHECK_EQUAL(dc.read(), v);
    }
  }
  std::fill(std::begin(buffer), std::end(buffer), 0);

  { // 333,0,0,0,0,0
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorCompressor co(bw);
    std::list<dariadb::Value> values{};
    dariadb::Value delta = dariadb::Value(333);

    for (int i = 0; i < 10; i++) {
      co.append(delta);
      values.push_back(delta);
      delta = 0;
    }

    auto read_bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorDeCompressor dc(read_bw, values.front());
    values.pop_front();
    for (auto &v : values) {
      auto readed_value = dc.read();
      BOOST_CHECK_EQUAL(readed_value, v);
    }
  }

  { // maxValue
    std::fill(rng.begin, rng.end, 0);
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorCompressor co(bw);
    auto max_value = std::numeric_limits<dariadb::Value>::max();
    auto v1 = dariadb::Value(0);
    auto v2 = dariadb::Value(max_value);

    co.append(v1);
    co.append(v2);

    auto read_bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorDeCompressor dc(read_bw, 0);
    BOOST_CHECK_EQUAL(dc.read(), v2);
  }
}

BOOST_AUTO_TEST_CASE(FlagV2Compressor) {
  { // 1,2,3,4,5...
    const size_t test_buffer_size = 1000;
    uint8_t buffer[test_buffer_size];
    std::fill(std::begin(buffer), std::end(buffer), 0);

    dariadb::compression::Range rng{std::begin(buffer), std::end(buffer)};
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::FlagCompressor fc(bw);

    std::list<dariadb::Flag> flags{};
    dariadb::Flag delta = 1;
    for (int i = 0; i < 10; i++) {
      fc.append(delta);
      flags.push_back(delta);
      delta++;
    }

    auto dbw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::FlagDeCompressor fd(dbw, flags.front());
    flags.pop_front();
    for (auto f : flags) {
      auto v = fd.read();
      BOOST_CHECK_EQUAL(v, f);
    }
  }
}

BOOST_AUTO_TEST_CASE(CompressedBlockV2Test) {
  const size_t test_buffer_size = 513;

  uint8_t b_begin[test_buffer_size];
  auto b_end = std::end(b_begin);

  std::fill(b_begin, b_end, 0);
  dariadb::compression::Range rng{b_begin, b_end};

  using dariadb::compression::CopmressedWriter;
  using dariadb::compression::CopmressedReader;

  auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);

  CopmressedWriter cwr(bw);

  std::list<dariadb::Meas> meases{};
  auto zer_t = dariadb::timeutil::current_time();
  for (int i = 0;; i++) {
    auto m = dariadb::Meas(1);
    m.time = zer_t++;
    m.flag = i;
    m.value = i;
    if (!cwr.append(m)) {
      BOOST_CHECK(cwr.isFull());
      break;
    }
    meases.push_back(m);
  }
  BOOST_CHECK_LE(cwr.usedSpace(), test_buffer_size);

  auto rbw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
  CopmressedReader crr(rbw, meases.front());

  meases.pop_front();
  for (auto &m : meases) {
    auto r_m = crr.read();
    BOOST_CHECK(m.flag == r_m.flag);
    BOOST_CHECK(m.id == r_m.id);
    BOOST_CHECK(m.time == r_m.time);
    BOOST_CHECK(m.value == r_m.value);
  }
}

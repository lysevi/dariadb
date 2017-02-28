#include <cstdlib>
#include <ctime>
#include <iostream>
#include <iterator>

#include <libdariadb/compression/compression.h>
#include <libdariadb/compression/delta.h>
#include <libdariadb/compression/flag.h>
#include <libdariadb/compression/xor.h>
#include <libdariadb/timeutil.h>
#include <chrono>
#include <cmath>
#include <ctime>
#include <limits>

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;

  auto test_buffer_size = 1024 * 1024 * 100;
  uint8_t *buffer = new uint8_t[test_buffer_size];
  dariadb::compression::Range rng{buffer, buffer + test_buffer_size};
  std::fill(buffer, buffer + test_buffer_size, 0);

  {
    const size_t count = 1000000;

    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);

    dariadb::compression::DeltaCompressor dc(bw);

    std::vector<dariadb::Time> deltas{50, 2553, 1000, 2000, 500};
    dariadb::Time t = 0;
    auto start = clock();
    for (size_t i = 0; i < count; i++) {
      dc.append(t);
      t += deltas[i % deltas.size()];
      if (t > dariadb::MAX_TIME) {
        t = 0;
      }
    }
    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "delta compressor : " << elapsed << std::endl;

    auto w = dc.used_space();
    auto sz = sizeof(dariadb::Time) * count;
    std::cout << "used space:  " << (w * 100.0) / (sz) << "%" << std::endl;
  }
  {
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::DeltaDeCompressor dc(bw, 0);

    auto start = clock();
    for (size_t i = 1; i < 1000000; i++) {
      dc.read();
    }
    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "delta decompressor : " << elapsed << std::endl;
  }
  // xor compression
  std::fill(buffer, buffer + test_buffer_size, 0);
  {
    const size_t count = 1000000;
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorCompressor dc(bw);

    dariadb::Value t = 3.14;
    auto start = clock();
    for (size_t i = 0; i < count; i++) {
      dc.append(t);
      t *= 1.5;
    }
    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "\nxor compressor : " << elapsed << std::endl;
    auto w = dc.used_space();
    auto sz = sizeof(dariadb::Time) * count;
    std::cout << "used space: " << (w * 100.0) / (sz) << "%" << std::endl;
  }
  {
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::XorDeCompressor dc(bw, 0);

    auto start = clock();
    for (size_t i = 1; i < 1000000; i++) {
      dc.read();
    }
    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "xor decompressor : " << elapsed << std::endl;
  }

  // flag compression
  std::fill(buffer, buffer + test_buffer_size, 0);
  {
    const size_t count = 1000000;
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::FlagCompressor dc(bw);

    dariadb::Flag t = 1;
    auto start = clock();
    for (size_t i = 0; i < count / 2; i++) {
      dc.append(t);
      dc.append(t);
      t++;
    }
    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "\nflag compressor : " << elapsed << std::endl;
    auto w = dc.used_space();
    auto sz = sizeof(dariadb::Time) * count;
    std::cout << "used space: " << (w * 100.0) / (sz) << "%" << std::endl;
  }
  {
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::FlagDeCompressor dc(bw, 0);

    auto start = clock();
    for (size_t i = 1; i < 1000000; i++) {
      dc.read();
    }
    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "flag decompressor : " << elapsed << std::endl;
  }

  // flag compression
  std::fill(buffer, buffer + test_buffer_size, 0);
  {
    const size_t count = 1000000;
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::FlagCompressor dc(bw);

    dariadb::Flag t = 1;
    auto start = clock();
    for (size_t i = 0; i < count / 2; i++) {
      dc.append(t);
      dc.append(t);
      t++;
    }
    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "\nid compressor : " << elapsed << std::endl;
    auto w = dc.used_space();
    auto sz = sizeof(dariadb::Time) * count;
    std::cout << "used space: " << (w * 100.0) / (sz) << "%" << std::endl;
  }

  {
    const size_t count = 1000000;
    uint8_t *buf_begin = new uint8_t[test_buffer_size];

    std::fill(buf_begin, buf_begin + test_buffer_size, 0);
    auto bw = std::make_shared<dariadb::compression::ByteBuffer>(rng);

    dariadb::compression::CopmressedWriter cwr{bw};
    auto start = clock();
    for (size_t i = 0; i < count; i++) {
      auto m = dariadb::Meas();
      m.id++;
      m.time = static_cast<dariadb::Time>(dariadb::timeutil::current_time());
      m.flag = dariadb::Flag(i);
      m.value = dariadb::Value(i);
      cwr.append(m);
    }

    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "\ncompress writer : " << elapsed << std::endl;
    auto w = cwr.usedSpace();
    auto sz = sizeof(dariadb::Meas) * count;
    std::cout << "used space: " << (w * 100.0) / (sz) << "%" << std::endl;

    auto m = dariadb::Meas();

    auto rbw = std::make_shared<dariadb::compression::ByteBuffer>(rng);
    dariadb::compression::CopmressedReader crr{rbw, m};

    start = clock();
    for (int i = 1; i < 1000000; i++) {
      crr.read();
    }
    elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "compress reader : " << elapsed << std::endl;

    delete[] buf_begin;
  }
  delete[] buffer;
}

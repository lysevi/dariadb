#include <cstdlib>
#include <ctime>
#include <iostream>
#include <iterator>

#include <dariadb.h>
#include <chrono>
#include <cmath>
#include <compression/compression.h>
#include <compression/delta.h>
#include <compression/flag.h>
#include <compression/id.h>
#include <compression/xor.h>
#include <ctime>
#include <limits>

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;

  auto test_buffer_size = 1024 * 1024 * 100;
  uint8_t *buffer = new uint8_t[test_buffer_size];
  dariadb::utils::Range rng{buffer, buffer + test_buffer_size};
  // delta compression
  std::fill(buffer, buffer + test_buffer_size, 0);

  {
    const size_t count = 1000000;

    auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);

    dariadb::compression::DeltaCompressor dc(bw);

    std::vector<dariadb::Time> deltas{50, 255, 1024, 2050};
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
    auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);
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
    auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);
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
    auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);
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
    auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);
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
    auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);
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
    auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);
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
    auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);
    dariadb::compression::IdDeCompressor dc(bw, 0);

    auto start = clock();
    for (size_t i = 1; i < 1000000; i++) {
      dc.read();
    }
    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "id decompressor : " << elapsed << std::endl;
  }

  {
    const size_t count = 1000000;
    auto start = clock();
    for (size_t i = 0; i < count; i++) {
      dariadb::compression::inner::flat_double_to_int(3.14);
    }
    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "\nflat_double_to_int: " << elapsed << std::endl;

    start = clock();
    for (size_t i = 0; i < count; i++) {
      dariadb::compression::inner::flat_int_to_double(0xfff);
    }
    elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "flat_int_to_double: " << elapsed << std::endl;
  }

  {
    const size_t count = 1000000;
    uint8_t *buf_begin = new uint8_t[test_buffer_size];

    std::fill(buf_begin, buf_begin + test_buffer_size, 0);
    auto bw = std::make_shared<dariadb::compression::BinaryBuffer>(rng);

    dariadb::compression::CopmressedWriter cwr{bw};
    auto start = clock();
    for (size_t i = 0; i < count; i++) {
      auto m = dariadb::Meas::empty();
      m.id++;
      m.time = static_cast<dariadb::Time>(dariadb::timeutil::current_time());
      m.flag = dariadb::Flag(i);
      m.value = dariadb::Value(i);
      cwr.append(m);
    }

    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "\ncompress writer : " << elapsed << std::endl;
    auto w = cwr.used_space();
    auto sz = sizeof(dariadb::Meas) * count;
    std::cout << "used space: " << (w * 100.0) / (sz) << "%" << std::endl;

    auto m = dariadb::Meas::empty();
    bw->reset_pos();
    dariadb::compression::CopmressedReader crr{bw, m};

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

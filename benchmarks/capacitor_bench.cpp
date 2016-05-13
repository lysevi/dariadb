#include <cstdlib>
#include <ctime>
#include <iostream>
#include <iterator>

#include <ctime>
#include <storage/capacitor.h>
#include <timeutil.h>
#include <utils/fs.h>

class Moc_Storage : public dariadb::storage::BaseStorage {
public:
  dariadb::append_result append(const dariadb::Meas &) override {
    return dariadb::append_result(1, 0);
  }

  dariadb::Time minTime() override { return 0; }
  /// max time of writed meas
  dariadb::Time maxTime() override { return 0; }

  bool minMaxTime(dariadb::Id, dariadb::Time *, dariadb::Time *) override {
    return 0;
  }

  void subscribe(const dariadb::IdArray &, const dariadb::Flag &,
                 const dariadb::storage::ReaderClb_ptr &) override {}
  dariadb::storage::Reader_ptr currentValue(const dariadb::IdArray &,
                                            const dariadb::Flag &) override {
    return nullptr;
  }

  void flush() override {}

  dariadb::storage::Cursor_ptr
  chunksByIterval(const dariadb::storage::QueryInterval &query) override {
    return nullptr;
  }

  dariadb::storage::IdToChunkMap
  chunksBeforeTimePoint(const dariadb::storage::QueryTimePoint &) override {
    return dariadb::storage::IdToChunkMap{};
  }
  dariadb::IdArray getIds() override { return dariadb::IdArray{}; }
};

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;

  const size_t K = 1;
  {
    const std::string storage_path = "testStorage";
    const size_t cap_B = 128 * 1024 / sizeof(dariadb::Meas);
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    const size_t id_count = 10;

    std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
    dariadb::storage::Capacitor tos{
        stor, dariadb::storage::Capacitor::Params(cap_B, storage_path)};

    auto m = dariadb::Meas::empty();

    auto start = clock();

    for (size_t i = 0; i < K * 1000000; i++) {
      m.id = i % id_count;
      m.flag = 0xff;
      m.time = dariadb::timeutil::current_time();
      m.value = dariadb::Value(i);
      tos.append(m);
    }

    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "Capacitor insert : " << elapsed << std::endl;
  }
}

#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <storage/time_ordered_set.h>
#include <timeutil.h>
#include <storage/capacitor.h>
#include <ctime>

class Moc_Storage :public dariadb::storage::BaseStorage {
public:
	dariadb::append_result append(const dariadb::Meas::PMeas, const size_t size) {
		return dariadb::append_result(size,0);
	}
	dariadb::append_result append(const dariadb::Meas &) {
		return dariadb::append_result(1,0);
	}

	dariadb::Time minTime() {
		return 0;
	}
	/// max time of writed meas
	dariadb::Time maxTime() {
		return 0;
	}

    void subscribe(const dariadb::IdArray&,const dariadb::Flag& , const dariadb::storage::ReaderClb_ptr &) override {
	}
	dariadb::storage::Reader_ptr currentValue(const dariadb::IdArray&, const dariadb::Flag&) {
		return nullptr;
	}

	void flush()override {
	}
	
	dariadb::storage::Cursor_ptr chunksByIterval(const dariadb::IdArray &, dariadb::Flag, dariadb::Time, dariadb::Time) {
		return nullptr;
	}

	dariadb::storage::IdToChunkMap chunksBeforeTimePoint(const dariadb::IdArray &, dariadb::Flag , dariadb::Time ) {
		return dariadb::storage::IdToChunkMap{};
	}
	dariadb::IdArray getIds() { return dariadb::IdArray{}; }
};

int main(int argc, char *argv[]) {
	(void)argc;
	(void)argv;

    const size_t K = 1;

    {
        const size_t max_size=K*100000;
        auto tos =dariadb::storage::TimeOrderedSet{ max_size };
        auto m = dariadb::Meas::empty();

        auto start = clock();


        for (size_t i = 0; i < max_size; i++) {
            m.id = 1;
            m.flag = 0xff;
            m.time = i;
            m.value = dariadb::Value(i);
            tos.append(m);
        }

        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"TimeOrderedSet insert : "<<elapsed<<std::endl;

        start = clock();
        auto reader = tos.as_array();

        elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
        std::cout << "TimeOrderedSet as_array: " << elapsed << std::endl;
        std::cout << "readed: " << reader.size() << std::endl;
    }

    {
        const size_t max_size=10000;
        const size_t id_count=10;
        const dariadb::Time write_window_deep = 2000;

		std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
        dariadb::storage::Capacitor tos{ stor, dariadb::storage::Capacitor::Params(max_size,write_window_deep)};
        auto m = dariadb::Meas::empty();

        auto start = clock();


        for (size_t i = 0; i < K*1000000; i++) {
            m.id = i%id_count;
            m.flag = 0xff;
            m.time = dariadb::timeutil::current_time();
			m.value = dariadb::Value(i);
            tos.append(m);
        }

        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"Bucket insert : "<<elapsed<<std::endl;
        std::cout<<"size : "<<tos.size()<<std::endl;
    }
}

#include <ctime>
#include <iostream>
#include <cstdlib>
#include <iterator>

#include <time_ordered_set.h>
#include <timeutil.h>
#include <capacitor.h>
#include <ctime>

class Moc_Storage :public memseries::storage::AbstractStorage {
public:
	memseries::append_result append(const memseries::Meas::PMeas begin, const size_t size) {
		return memseries::append_result(size,0);
	}
	memseries::append_result append(const memseries::Meas &value) {
		return memseries::append_result(1,0);
	}
	memseries::storage::Reader_ptr readInterval(const memseries::IdArray &ids, memseries::Flag flag, memseries::Time from, memseries::Time to) {
		return nullptr;
	}

	memseries::storage::Reader_ptr readInTimePoint(const memseries::IdArray &ids, memseries::Flag flag, memseries::Time time_point) {
		return nullptr;
	}
	memseries::Time minTime() {
		return 0;
	}
	/// max time of writed meas
	memseries::Time maxTime() {
		return 0;
	}

	void subscribe(const memseries::IdArray&ids, memseries::Flag flag, memseries::storage::ReaderClb_ptr clbk) override {
	}
};

int main(int argc, char *argv[]) {
    const size_t K = 1;

    {
        const size_t max_size=K*100000;
        auto tos =memseries::storage::TimeOrderedSet{ max_size };
        auto m = memseries::Meas::empty();

        auto start = clock();


        for (size_t i = 0; i < max_size; i++) {
            m.id = 1;
            m.flag = 0xff;
            m.time = i;
            m.value = i;
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
        const memseries::Time write_window_deep = 2000;

		std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
        auto tos =memseries::storage::Capacitor{ max_size,stor, write_window_deep };
        auto m = memseries::Meas::empty();

        auto start = clock();


        for (size_t i = 0; i < K*1000000; i++) {
            m.id = i%id_count;
            m.flag = 0xff;
            m.time = memseries::timeutil::current_time();
            m.value = i;
            tos.append(m);
        }

        auto elapsed=((float)clock()-start)/ CLOCKS_PER_SEC;
        std::cout<<"Bucket insert : "<<elapsed<<std::endl;
        std::cout<<"size : "<<tos.size()<<std::endl;
    }
}

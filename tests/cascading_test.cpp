#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <set>
#include <ctime>
#include <random>
#include <utils/exception.h>
#include <utils/cola.h>

using namespace dariadb::utils;

BOOST_AUTO_TEST_CASE(Cascading_insert) {
    cascading c;
    c.push(3);
    BOOST_CHECK_EQUAL(c.levels_count(),size_t(1));
    c.push(1);
    cascading::item out_res;
    c.find(1,&out_res);
    BOOST_CHECK_EQUAL(out_res.value,1);
    BOOST_CHECK_EQUAL(c.levels_count(),size_t(2));
    //c.print();
    c.push(2);
    BOOST_CHECK_EQUAL(c.levels_count(),size_t(2));
    //c.print();
    c.push(4);
    c.find(4,&out_res);
    BOOST_CHECK_EQUAL(out_res.value,4);
    //c.print();
    BOOST_CHECK_EQUAL(c.levels_count(),size_t(3));
    c.push(7);
    c.push(11);
    BOOST_CHECK_EQUAL(c.levels_count(),size_t(3));
    //c.print();
    c.push(5);
    c.push(15);
    BOOST_CHECK_EQUAL(c.levels_count(),size_t(4));
    //c.print();
    c.find(1,&out_res);
    BOOST_CHECK_EQUAL(out_res.value,1);
    c.find(15,&out_res);
    BOOST_CHECK_EQUAL(out_res.value,15);
}


BOOST_AUTO_TEST_CASE(Cascading_insert_big) {

    const size_t insertion_count=50000;
    std::vector<int> keys(insertion_count);
    std::uniform_int_distribution<int> distribution(0, insertion_count * 10);
    std::mt19937 engine;
    auto generator = std::bind(distribution, engine);
    std::generate_n(keys.begin(), insertion_count, generator);

    {
        cascading c;
        c.resize(16);
        auto start = clock();
        for(size_t i=0;i<insertion_count;i++){
            c.push(keys[i]);
        }
        auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
        std::cout<<"lvls: "<<c.levels_count()<<std::endl;
        std::cout << "cascading insert : " << elapsed << std::endl;

        start = clock();
        for(size_t i=0;i<insertion_count;i++){
            cascading::item res;
            if(!c.find(keys[i],&res)){
                throw MAKE_EXCEPTION("!c.find(keys[i],&res)");
            }
        }
        elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
        std::cout << "cascading find : " << elapsed << std::endl;
    }

    {
        std::set<int> c;
        auto start = clock();
        for(size_t i=0;i<insertion_count;i++){
            c.insert(keys[i]);
        }
        auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
        std::cout << "set insert : " << elapsed << std::endl;

        start = clock();
        for(size_t i=0;i<insertion_count;i++){
            if(c.find(keys[i])==c.end()){
                throw MAKE_EXCEPTION("c.find(keys[i])==c.end()");
            }
        }
        elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
        std::cout << "set find : " << elapsed << std::endl;
    }
}

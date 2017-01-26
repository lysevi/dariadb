#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <libdariadb/storage/clmn/nodes.h>
#include <libdariadb/storage/clmn/tree.h>
#include <libdariadb/utils/fs.h>

#include <boost/test/unit_test.hpp>

using namespace dariadb::storage;

void checkNodeCtor(const clmn::Node::Ptr &n, clmn::gnrt_t expected_g,
                   clmn::node_id_t expected_i, clmn::node_sz_t expected_s) {

  BOOST_CHECK_EQUAL(n->neighbor, clmn::NODE_PTR_NULL);
  BOOST_CHECK_EQUAL(n->hdr.gen, expected_g);
  BOOST_CHECK_EQUAL(n->hdr.id, expected_i);
  BOOST_CHECK_EQUAL(n->hdr.size, expected_s);
  BOOST_CHECK(!n->children_is_leaf);
  for (clmn::node_sz_t i = 0; i < expected_s; ++i) {
    BOOST_CHECK_EQUAL(n->children[i], clmn::NODE_PTR_NULL);
  }
}

BOOST_AUTO_TEST_CASE(Footer) {
  const char *expected = "dariadb.";
  auto number_representation = clmn::MAGIC_NUMBER;
  char *magic_str = (char *)&number_representation;
  for (size_t i = 0; i < sizeof(uint64_t); ++i) {
    BOOST_CHECK_EQUAL(magic_str[i], expected[i]);
  }

  const clmn::node_id_t expected_i = 2;
  const clmn::node_addr_t expected_a = 0xdeadbeef;

  clmn::Footer::Footer_Ptr ftr =
      clmn::Footer::make_footer(expected_i, expected_a);
  BOOST_CHECK_EQUAL(ftr->crc, uint32_t(0));
  BOOST_CHECK_EQUAL(ftr->magic_num, clmn::MAGIC_NUMBER);
  BOOST_CHECK_EQUAL(ftr->hdr.meas_id, expected_i);
  BOOST_CHECK_EQUAL(ftr->hdr.kind, clmn::NODE_KIND_FOOTER);
  BOOST_CHECK_EQUAL(ftr->hdr.size, uint32_t(1));
  BOOST_CHECK_EQUAL(ftr->hdr.gen, clmn::gnrt_t(0));
  BOOST_CHECK_EQUAL(ftr->max_node_id(), clmn::node_id_t(0));

  for (clmn::node_sz_t i = 0; i < uint32_t(1); ++i) {
    BOOST_CHECK_EQUAL(ftr->roots[i], expected_a);
  }

  for (clmn::node_sz_t i = 0; i < uint32_t(1); ++i) {
    BOOST_CHECK_EQUAL(ftr->ids[i], expected_i);
  }

  { // copy test;
    clmn::Footer::Footer_Ptr second_f =
        clmn::Footer::copy_on_write(ftr, expected_i, expected_a + 1);

	BOOST_CHECK_EQUAL(ftr->hdr.meas_id, expected_i);
    BOOST_CHECK_EQUAL(second_f->hdr.gen, clmn::gnrt_t(1));

    for (clmn::node_sz_t i = 0; i < uint32_t(1); ++i) {
      BOOST_CHECK_EQUAL(second_f->roots[i], expected_a + 1);
    }

    for (clmn::node_sz_t i = 0; i < uint32_t(1); ++i) {
      BOOST_CHECK_EQUAL(second_f->ids[i], expected_i);
    }
  }
}

BOOST_AUTO_TEST_CASE(StatisticUpdate) {
  clmn::Statistic st;

  BOOST_CHECK_EQUAL(st.min_time, dariadb::MAX_TIME);
  BOOST_CHECK_EQUAL(st.max_time, dariadb::MIN_TIME);
  BOOST_CHECK_EQUAL(st.count, uint32_t(0));
  BOOST_CHECK_EQUAL(st.flg_bloom, dariadb::Flag(0));
  BOOST_CHECK_EQUAL(st.min_value, dariadb::MAX_VALUE);
  BOOST_CHECK_EQUAL(st.max_value, dariadb::MIN_VALUE);
  BOOST_CHECK_EQUAL(st.sum, dariadb::MIN_VALUE);
  auto m = dariadb::Meas::empty(0);
  m.time = 2;
  m.flag = 2;
  m.value = 2;
  st.update(m);
  BOOST_CHECK_EQUAL(st.min_time, m.time);
  BOOST_CHECK_EQUAL(st.max_time, m.time);
  BOOST_CHECK(st.flg_bloom != dariadb::Flag(0));
  BOOST_CHECK(dariadb::areSame(st.min_value, m.value));
  BOOST_CHECK(dariadb::areSame(st.max_value, m.value));

  m.time = 3;
  m.value = 3;
  st.update(m);
  BOOST_CHECK_EQUAL(st.min_time, dariadb::Time(2));
  BOOST_CHECK_EQUAL(st.max_time, dariadb::Time(3));
  BOOST_CHECK(dariadb::areSame(st.min_value, dariadb::Value(2)));
  BOOST_CHECK(dariadb::areSame(st.max_value, dariadb::Value(3)));

  m.time = 1;
  m.value = 1;
  st.update(m);
  BOOST_CHECK_EQUAL(st.min_time, dariadb::Time(1));
  BOOST_CHECK_EQUAL(st.max_time, dariadb::Time(3));
  BOOST_CHECK(dariadb::areSame(st.min_value, dariadb::Value(1)));
  BOOST_CHECK(dariadb::areSame(st.max_value, dariadb::Value(3)));
  BOOST_CHECK(dariadb::areSame(st.sum, dariadb::Value(6)));
  BOOST_CHECK_EQUAL(st.count, uint32_t(3));

  clmn::Statistic second_st;
  m.time = 777;
  m.value = 1;
  second_st.update(m);
  BOOST_CHECK_EQUAL(second_st.max_time, m.time);

  second_st.update(st);
  BOOST_CHECK_EQUAL(second_st.min_time, dariadb::Time(1));
  BOOST_CHECK_EQUAL(second_st.max_time, dariadb::Time(777));
  BOOST_CHECK(dariadb::areSame(second_st.min_value, dariadb::Value(1)));
  BOOST_CHECK(dariadb::areSame(second_st.max_value, dariadb::Value(3)));
  BOOST_CHECK(dariadb::areSame(second_st.sum, dariadb::Value(7)));
  BOOST_CHECK_EQUAL(second_st.count, uint32_t(4));
}

BOOST_AUTO_TEST_CASE(LeafAndNode) {
  const clmn::gnrt_t expected_g = 1;
  const clmn::node_id_t expected_i = 2;
  const clmn::node_sz_t expected_s = 3;

  {
    clmn::Node::Ptr r =
        clmn::Node::make_root(expected_g, expected_i, expected_s);

    BOOST_CHECK_EQUAL(r->hdr.kind, clmn::NODE_KIND_ROOT);
    checkNodeCtor(r, expected_g, expected_i, expected_s);
  }

  {
    clmn::Node::Ptr n =
        clmn::Node::make_node(expected_g, expected_i, expected_s);

    BOOST_CHECK_EQUAL(n->hdr.kind, clmn::NODE_KIND_NODE);
    checkNodeCtor(n, expected_g, expected_i, expected_s);
  }

  {
    clmn::Leaf::Ptr l =
        clmn::Leaf::make_leaf(expected_g, expected_i, expected_s);

    BOOST_CHECK_EQUAL(l->neighbor, clmn::NODE_PTR_NULL);
    BOOST_CHECK_EQUAL(l->hdr.kind, clmn::NODE_KIND_LEAF);
    BOOST_CHECK_EQUAL(l->hdr.gen, expected_g);
    BOOST_CHECK_EQUAL(l->hdr.id, expected_i);
    BOOST_CHECK_EQUAL(l->hdr.size, expected_s);
  }
}

BOOST_AUTO_TEST_CASE(MemoryNodeStorageTest) {
  {
    const dariadb::Id expected_id = dariadb::Id(1);

    auto msn = clmn::MemoryNodeStorage::create();
    auto r = clmn::Node::make_root(clmn::gnrt_t(0), clmn::node_id_t(1),
                                   clmn::node_sz_t(1));
    r->hdr.meas_id = expected_id;

    auto n1 = clmn::Node::make_node(clmn::gnrt_t(0), clmn::node_id_t(1),
                                    clmn::node_sz_t(1));
    n1->hdr.meas_id = expected_id;
    auto n2 = clmn::Node::make_node(clmn::gnrt_t(0), clmn::node_id_t(1),
                                    clmn::node_sz_t(1));
    n2->hdr.meas_id = expected_id;
    auto l1 = clmn::Leaf::make_leaf(clmn::gnrt_t(0), clmn::node_id_t(1),
                                    clmn::node_sz_t(1));

    l1->hdr.meas_id = expected_id;
    auto l2 = clmn::Leaf::make_leaf(clmn::gnrt_t(0), clmn::node_id_t(1),
                                    clmn::node_sz_t(1));
    l2->hdr.meas_id = expected_id;

    BOOST_CHECK(msn->getFooter() == nullptr);
    auto addrs = msn->write(clmn::leaf_vector{l1, l2});
    BOOST_CHECK_EQUAL(addrs.size(), size_t(2));
    for (auto a : addrs) {
      BOOST_CHECK(a != clmn::NODE_PTR_NULL);
    }
	//insert first
    msn->write(clmn::node_vector{r, n1, n2});
    auto footer = msn->getFooter();
    BOOST_CHECK(footer != nullptr);

    BOOST_CHECK_EQUAL(footer->hdr.size, clmn::node_sz_t(1));
    BOOST_CHECK(footer->roots[0] != clmn::NODE_PTR_NULL);
	
	//insert one more time
    msn->write(clmn::node_vector{r, n1, n2});
	footer = msn->getFooter();
    BOOST_CHECK_EQUAL(footer->hdr.size, clmn::node_sz_t(1));
    BOOST_CHECK(footer->roots[0] != clmn::NODE_PTR_NULL);

	//insert new root
	r->hdr.meas_id++;
	msn->write(clmn::node_vector{ r, n1, n2 });
	footer = msn->getFooter();
	BOOST_CHECK_EQUAL(footer->hdr.size, clmn::node_sz_t(2));
	BOOST_CHECK(footer->roots[0] != clmn::NODE_PTR_NULL);
	BOOST_CHECK(footer->roots[1] != clmn::NODE_PTR_NULL);
  }
}
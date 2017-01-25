#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <libdariadb/storage/clmn/nodes.h>
#include <libdariadb/utils/fs.h>

#include <boost/test/unit_test.hpp>
using namespace dariadb::storage;

BOOST_AUTO_TEST_CASE(Footer) {
  char *expected = "dariadb.";
  auto number_representation = clmn::MAGIC_NUMBER;
  char *magic_str = (char *)&number_representation;
  for (size_t i = 0; i < sizeof(uint64_t); ++i) {
    BOOST_CHECK_EQUAL(magic_str[i], expected[i]);
  }

  clmn::Footer ftr;
  BOOST_CHECK_EQUAL(ftr.magic_num, clmn::MAGIC_NUMBER);
  BOOST_CHECK_EQUAL(ftr.hdr.kind, clmn::NODE_KIND_FOOTER);
  BOOST_CHECK_EQUAL(ftr.hdr.size, uint32_t(0));
  BOOST_CHECK_EQUAL(ftr.hdr.gen, clmn::generation_t(0));
  BOOST_CHECK_EQUAL(ftr.max_node_id(), clmn::node_id_t(0));
}

BOOST_AUTO_TEST_CASE(Node) {
  clmn::generation_t expected_g = 1;
  clmn::node_id_t expected_i = 2;
  clmn::node_size_t expected_s = 3;
  {
    clmn::Node r = clmn::Node::make_root(expected_g, expected_i, expected_s);

    BOOST_CHECK_EQUAL(r.hdr.kind, clmn::NODE_KIND_ROOT);
    BOOST_CHECK_EQUAL(r.hdr.gen, expected_g);
    BOOST_CHECK_EQUAL(r.hdr.id, expected_i);
    BOOST_CHECK_EQUAL(r.hdr.size, expected_s);
  }

  {
    clmn::Node n = clmn::Node::make_node(expected_g, expected_i, expected_s);

    BOOST_CHECK_EQUAL(n.hdr.kind, clmn::NODE_KIND_NODE);
    BOOST_CHECK_EQUAL(n.hdr.gen, expected_g);
    BOOST_CHECK_EQUAL(n.hdr.id, expected_i);
    BOOST_CHECK_EQUAL(n.hdr.size, expected_s);
  }
}
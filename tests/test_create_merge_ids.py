import json
import os
import shutil
import unittest

from utils.create_merge_ids import create_match_keys, create_match_sets

static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


class TestGetCombinedMap(unittest.TestCase):
    maxDiff = None

    # TODO: refactor to read expected outputs from file too and eliminate the boilerplate
    def test_get_combined_map1(self):
        match_dir = os.path.join(static_dir, "test_get_combined_map1")
        expected_result = [{"A", "B", "C"}]
        self.assertEqual(create_match_sets(match_dir), expected_result)

    def test_get_combined_map2(self):
        match_dir = os.path.join(static_dir, "test_get_combined_map2")
        expected_result = [{"A", "B", "C", "D"}]
        self.assertEqual(create_match_sets(match_dir), expected_result)

    def test_get_combined_map3(self):
        match_dir = os.path.join(static_dir, "test_get_combined_map3")
        expected_result = [{"A", "B", "C", "D", "E"}]
        self.assertEqual(create_match_sets(match_dir), expected_result)

    def test_get_combined_map4(self):
        match_dir = os.path.join(static_dir, "test_get_combined_map4")
        expected_result = [{"A", "B", "C", "D", "E", "F", "G", "H"}]
        self.assertEqual(create_match_sets(match_dir), expected_result)

    def test_get_combined_map5(self):
        # test with two disconnected sets
        match_dir = os.path.join(static_dir, "test_get_combined_map5")
        result_set_large = {"A", "B", "C", "D", "E"}
        result_set_small = {"F", "G", "H"}
        expected_result = sorted(
            [result_set_small, result_set_large], key=lambda k: len(k)
        )
        actual_result = sorted(create_match_sets(match_dir), key=lambda k: len(k))
        self.assertEqual(actual_result, expected_result)

    def test_get_match_sets_with_extra_id(self):
        # test with three disconnected sets. The set A - E will have one extra id (E) that should get filtered, and
        # the "small" set F-H will all be extra ids that should be filtered. The other small set I-J will have ids
        # distributed across two id files, but the set should be included.
        match_dir = os.path.join(
            static_dir, "test_get_match_sets_with_extra_id", "match_pairs"
        )
        ids_dir = os.path.join(static_dir, "test_get_match_sets_with_extra_id", "ids")
        result_set_large = {"A", "B", "C", "D"}
        result_set_small = {"I", "J"}
        expected_result = sorted(
            [result_set_small, result_set_large], key=lambda k: len(k)
        )
        actual_result = sorted(
            create_match_sets(match_dir, ids_dir), key=lambda k: len(k)
        )
        self.assertEqual(actual_result, expected_result)

    def test_skip_matches(self):
        # test without matches excluded
        match_dir = os.path.join(static_dir, "test_skip_matches_ids")
        expected_result_no_excludes = [{"A", "B", "C"}, {"D", "E"}]
        self.assertEqual(create_match_sets(match_dir), expected_result_no_excludes)
        # test with matches excluded
        exclude_dir = os.path.join(static_dir, "test_skip_matches_ids_to_skip")
        expected_result_excludes = [{"A", "B"}, {"C"}, {"D"}, {"E"}]
        self.assertEqual(create_match_sets(match_dir, exclude_dir=exclude_dir), expected_result_excludes)

    def test_create_match_keys(self):
        # the first set will contain two old elts from the same match set and one new elt; should keep its id
        # the next will contain one elt from one match set, two from another; should change ids
        # the last will contain only new ids; should get a new id
        match_sets = [{"A", "B", "C"}, {"D", "E", "F"}, {"G", "H"}]
        out_dir = os.path.join(static_dir, "test_create_match_keys", "output")
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
        os.mkdir(out_dir)
        out_fi = os.path.join(out_dir, "output.jsonl")
        id_mapping_dir = os.path.join(static_dir, "test_create_match_keys", "input")
        expected_output = [
            {"orig_id": "A", "merged_id": "carticle_0000000001"},
            {"orig_id": "B", "merged_id": "carticle_0000000001"},
            {"orig_id": "C", "merged_id": "carticle_0000000001"},
            {"orig_id": "D", "merged_id": "carticle_0000000003"},
            {"orig_id": "E", "merged_id": "carticle_0000000003"},
            {"orig_id": "F", "merged_id": "carticle_0000000003"},
            {"orig_id": "G", "merged_id": "carticle_0000000004"},
            {"orig_id": "H", "merged_id": "carticle_0000000004"},
        ]
        create_match_keys(match_sets, out_fi, id_mapping_dir)
        out = [json.loads(x) for x in open(out_fi).readlines()]
        self.assertEqual(expected_output, sorted(out, key=lambda x: x["orig_id"]))

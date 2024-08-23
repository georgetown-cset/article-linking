import json
import os
import shutil
import unittest

from utils.create_merge_ids import create_match_sets, create_matches

static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
empty_simhash_dir = os.path.join(static_dir, "simhash_empty")


class TestGetCombinedMap(unittest.TestCase):
    maxDiff = None

    # TODO: refactor to read expected outputs from file too and eliminate the boilerplate
    def test_get_combined_map1(self):
        match_dir = os.path.join(static_dir, "test_get_combined_map1")
        expected_result = [{"A", "B", "C"}]
        self.assertEqual(
            create_match_sets(match_dir, empty_simhash_dir), expected_result
        )

    def test_get_combined_map2(self):
        match_dir = os.path.join(static_dir, "test_get_combined_map2")
        expected_result = [{"A", "B", "C", "D"}]
        self.assertEqual(
            create_match_sets(match_dir, empty_simhash_dir), expected_result
        )

    def test_get_combined_map3(self):
        match_dir = os.path.join(static_dir, "test_get_combined_map3")
        expected_result = [{"A", "B", "C", "D", "E"}]
        self.assertEqual(
            create_match_sets(match_dir, empty_simhash_dir), expected_result
        )

    def test_get_combined_map4(self):
        match_dir = os.path.join(static_dir, "test_get_combined_map4")
        expected_result = [{"A", "B", "C", "D", "E", "F", "G", "H"}]
        self.assertEqual(
            create_match_sets(match_dir, empty_simhash_dir), expected_result
        )

    def test_get_combined_map5(self):
        # test with two disconnected sets
        match_dir = os.path.join(static_dir, "test_get_combined_map5")
        result_set_large = {"A", "B", "C", "D", "E"}
        result_set_small = {"F", "G", "H"}
        expected_result = sorted(
            [result_set_small, result_set_large], key=lambda k: len(k)
        )
        actual_result = sorted(
            create_match_sets(match_dir, empty_simhash_dir), key=lambda k: len(k)
        )
        self.assertEqual(actual_result, expected_result)

    def test_get_match_sets_with_extra_id(self):
        # test with three disconnected sets. The set A - E will have one simhash match (E-A) that should be included,
        # and matches involving the obsolete id D that should be filtered. The "small" set F-H contains simhash-only
        # ids and should be filtered. The other small set I-J should be included.
        match_dir = os.path.join(
            static_dir, "test_get_match_sets_with_extra_id", "match_pairs"
        )
        simhash_match_dir = os.path.join(
            static_dir, "test_get_match_sets_with_extra_id", "simhash_match_pairs"
        )
        result_set_large = {"A", "B", "C", "E"}
        result_set_small = {"I", "J"}
        expected_result = sorted(
            [result_set_small, result_set_large], key=lambda k: len(k)
        )
        actual_result = sorted(
            create_match_sets(match_dir, simhash_match_dir), key=lambda k: len(k)
        )
        self.assertEqual(actual_result, expected_result)

    def test_skip_matches(self):
        # test without matches excluded
        match_dir = os.path.join(static_dir, "test_skip_matches_ids")
        expected_result_no_excludes = [{"A", "B", "C"}, {"D", "E"}]
        self.assertEqual(
            create_match_sets(match_dir, empty_simhash_dir), expected_result_no_excludes
        )
        # test with matches excluded
        exclude_dir = os.path.join(static_dir, "test_skip_matches_ids_to_skip")
        expected_result_excludes = [{"A", "B"}, {"C"}, {"D"}, {"E"}]
        self.assertEqual(
            create_match_sets(match_dir, empty_simhash_dir, exclude_dir=exclude_dir),
            expected_result_excludes,
        )

    def test_create_matches(self):
        match_sets = [
            {"A", "B", "C"},
            {"D", "E", "F"},
            {"G", "H"},
            {"I"},
            {"J"},
            {"K", "L"},
            {"M", "N", "O"},
        ]
        id_mapping_dir = os.path.join(static_dir, "test_create_match_keys", "input")
        ids_to_drop = os.path.join(static_dir, "test_create_match_keys", "ids_to_drop")
        expected_output = [
            # F was removed from this match set so A B and C should get a new merged id
            {"orig_id": "A", "merged_id": "carticle_0000000006"},
            {"orig_id": "B", "merged_id": "carticle_0000000006"},
            {"orig_id": "C", "merged_id": "carticle_0000000006"},
            # D, E, F contains one elt from one match set, two from another; should change ids
            {"orig_id": "D", "merged_id": "carticle_0000000007"},
            {"orig_id": "E", "merged_id": "carticle_0000000007"},
            {"orig_id": "F", "merged_id": "carticle_0000000007"},
            # G, H is a completely new match set with new ids, should get a new id
            {"orig_id": "G", "merged_id": "carticle_0000000008"},
            {"orig_id": "H", "merged_id": "carticle_0000000008"},
            # The last two (I and J) are two different match sets that share an old id and are in ids_to_drop;
            # each should get a new id
            {"orig_id": "I", "merged_id": "carticle_0000000009"},
            {"orig_id": "J", "merged_id": "carticle_0000000010"},
            # Nothing changed for this match set so the merged id stays the same
            {"orig_id": "K", "merged_id": "carticle_0000000004"},
            {"orig_id": "L", "merged_id": "carticle_0000000004"},
            # This match set got one new article so the merged id stays the same
            {"orig_id": "M", "merged_id": "carticle_0000000005"},
            {"orig_id": "N", "merged_id": "carticle_0000000005"},
            {"orig_id": "O", "merged_id": "carticle_0000000005"},
        ]
        print(expected_output)
        match_batches = create_matches(match_sets, ids_to_drop, id_mapping_dir)
        matches = []
        for match_batch, batch_id in match_batches:
            matches.extend(match_batch)
        print(sorted(matches, key=lambda x: x["orig_id"]))
        self.assertEqual(expected_output, sorted(matches, key=lambda x: x["orig_id"]))

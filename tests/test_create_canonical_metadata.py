import unittest
import os
from create_merge_ids import create_match_sets

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
        expected_result = sorted([result_set_small, result_set_large], key=lambda k : len(k))
        actual_result = sorted(create_match_sets(match_dir), key=lambda k : len(k))
        self.assertEqual(actual_result, expected_result)


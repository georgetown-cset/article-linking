import unittest
import os
from ..create_canonical_metadata import create_match_sets, get_best_record

static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

class TestGetCombinedMap(unittest.TestCase):
    maxDiff = None

    def test_get_combined_map1(self):
        match_dir = os.path.join(static_dir, "test_get_combined_map1")
        result_set = {"A", "B", "C"}
        expected_result = {
            "A": result_set,
            "B": result_set,
            "C": result_set
        }
        self.assertEqual(create_match_sets(match_dir, "arxiv"), expected_result)

    def test_get_combined_map2(self):
        match_dir = os.path.join(static_dir, "test_get_combined_map2")
        result_set = {"A", "B", "C", "D"}
        expected_result = {
            "A": result_set,
            "B": result_set,
            "C": result_set,
            "D": result_set
        }
        self.assertEqual(create_match_sets(match_dir, "arxiv"), expected_result)

    def test_get_combined_map3(self):
        match_dir = os.path.join(static_dir, "test_get_combined_map3")
        result_set = {"A", "B", "C", "D", "E"}
        expected_result = {
            "A": result_set,
            "B": result_set,
            "C": result_set,
            "D": result_set,
            "E": result_set
        }
        self.assertEqual(create_match_sets(match_dir, "arxiv"), expected_result)

    def test_get_combined_map4(self):
        match_dir = os.path.join(static_dir, "test_get_combined_map4")
        result_set = {"A", "B", "C", "D", "E", "F", "G", "H"}
        expected_result = {
            "A": result_set,
            "B": result_set,
            "C": result_set,
            "D": result_set,
            "E": result_set,
            "F": result_set,
            "G": result_set,
            "H": result_set
        }
        self.assertEqual(create_match_sets(match_dir, "arxiv"), expected_result)

    def test_get_combined_map5(self):
        # test with two disconnected sets
        match_dir = os.path.join(static_dir, "test_get_combined_map5")
        result_set_large = {"A", "B", "C", "D", "E"}
        result_set_small = {"F", "G", "H"}
        expected_result = {
            "A": result_set_large,
            "B": result_set_large,
            "C": result_set_large,
            "D": result_set_large,
            "E": result_set_large,
            "F": result_set_small,
            "G": result_set_small,
            "H": result_set_small
        }
        self.assertEqual(create_match_sets(match_dir, "arxiv"), expected_result)

class TestGetBestRecord(unittest.TestCase):
    def test_get_best_record(self):
        record_list = [
            {"id": "A", "title": "", "abstract": "", "foo": "bar"},
            {"id": "B", "title": None, "abstract": None, "foo": ""},
            {"id": "C", "title": "title", "abstract": "abstract", "foo": None}
        ]
        self.assertEqual(get_best_record(record_list),
                         {"id": "C", "title": "title", "abstract": "abstract", "foo": "bar"})
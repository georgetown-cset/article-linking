import unittest
from ..create_canonical_metadata import get_combined_map, get_best_record

class TestGetCombinedMap(unittest.TestCase):
    def test_get_combined_map(self):
        match_map = {
            "A": {"B"},
            "B": {"C"}
        }
        self.assertIn(get_combined_map(match_map), [
            {
                "A": {"A", "B", "C"}
            },
            {
                "B": {"A", "B", "C"}
            }
        ])

    def test_get_combined_map1(self):
        match_map = {
            "A": {"B", "D"},
            "C": {"D"},
            "B": {"C"}
        }
        self.assertIn(get_combined_map(match_map), [
            {
                "A": {"A", "B", "C", "D"}
            },
            {
                "B": {"A", "B", "C", "D"}
            }
        ])

class TestGetBestRecord(unittest.TestCase):
    def test_get_best_record(self):
        record_list = [
            {"id": "A", "title": "", "abstract": "", "foo": "bar"},
            {"id": "B", "title": None, "abstract": None, "foo": ""},
            {"id": "C", "title": "title", "abstract": "abstract", "foo": None}
        ]
        self.assertEqual(get_best_record(record_list),
                         {"id": "C", "title": "title", "abstract": "abstract", "foo": "bar"})
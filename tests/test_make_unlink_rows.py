import json
import os
import shutil
import unittest

from utils.make_unlink_rows import make_pairs

static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


class TestMakeUnlinkRows(unittest.TestCase):
    @staticmethod
    def gen_sort_key(pair: tuple) -> str:
        return f"{pair[0]}-{pair[1]}"

    def test_make_pairs(self):
        manual_to_orig = {"1": {"a", "b"}, "2": {"d", "e"}, "3": {"f"}}
        expected_output = sorted(
            [
                ("a", "d"),
                ("a", "e"),
                ("a", "f"),
                ("b", "d"),
                ("b", "e"),
                ("b", "f"),
                ("d", "a"),
                ("d", "b"),
                ("d", "f"),
                ("e", "a"),
                ("e", "b"),
                ("e", "f"),
                ("f", "a"),
                ("f", "b"),
                ("f", "d"),
                ("f", "e"),
            ],
            key=self.gen_sort_key,
        )
        self.assertEqual(
            expected_output, sorted(make_pairs(manual_to_orig), key=self.gen_sort_key)
        )

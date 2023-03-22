import json
import unittest
from utils.clean_corpus import Scrub


class TestCleanCorpus(unittest.TestCase):
    scrubber = Scrub(["title", "abstract", "last_names"])
    maxDiff = None

    def test_strip_copyright_yes(self):
        self.assertEqual("test", self.scrubber.strip_copyright("test (C) 2014 test1"))
        self.assertEqual("test", self.scrubber.strip_copyright("test (c) 2014 test1"))
        self.assertEqual("test", self.scrubber.strip_copyright("test copyright 2014 test1"))

    def test_strip_copyright_no(self):
        self.assertEqual("test copyright test1", self.scrubber.strip_copyright("test copyright test1"))
        self.assertEqual("(a) the first item (b) the second item (c) the third item",
                         self.scrubber.strip_copyright("(a) the first item (b) the second item (c) the third item"))

    def test_clean_text_data(self):
        input_record = {
            "title": "Something un-normalized!",
            "abstract": "你好世界 copyright (C) 2014",
            "last_names": ["smith", "amy s. li", "界"]
        }
        expected_output_record = {
            "title": "Something un-normalized!",
            "abstract": "你好世界 copyright (C) 2014",
            "last_names": ["smith", "amy s. li", "界"],
            "title_norm": "somethingunnormalized",
            "abstract_norm": "你好世界",
            "last_names_norm": "li smith 界"
        }
        scrubber_output_generator = self.scrubber.process(json.dumps(input_record))
        self.assertEqual(json.dumps(expected_output_record), list(scrubber_output_generator)[0])

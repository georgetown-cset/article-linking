import unittest
from article_linking_methods.full_comparison import FullComparisonLinkerTitleFilter


class TestFullComparisonLinkerTitleFilterCalcPctOverlap(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.linker = FullComparisonLinkerTitleFilter(None, None, 0.4)

    def test_calculate_pct_overlap_one_empty(self):
        self.assertEqual(self.linker.empty, self.linker.calculate_pct_overlap("", "the quick brown fox"))

    def test_calculate_pct_overlap_both_empty(self):
        self.assertEqual(self.linker.empty, self.linker.calculate_pct_overlap("", ""))

    def test_calculate_pct_overlap_no_overlap(self):
        self.assertEqual(0.0, self.linker.calculate_pct_overlap("the quick brown fox", "jumped over a lazy dog"))

    def test_calculate_pct_overlap_short_substring(self):
        self.assertEqual(0.5, self.linker.calculate_pct_overlap("the quick brown", "the quick brown fox jumped over"))

    def test_calculate_pct_overlap_long_substring(self):
        self.assertEqual(self.linker.high_sim, self.linker.calculate_pct_overlap("the quick brown fox jumped over",
                                                                        "the quick brown fox jumped over a lazy dog"))

    def test_calculate_pct_overlap_perfect_match(self):
        self.assertEqual(1.0, self.linker.calculate_pct_overlap("the quick brown fox jumped over",
                                                                  "the quick brown fox jumped over"))


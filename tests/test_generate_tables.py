import unittest
import os
from generate_tables import get_sql_sequence

static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

class TestGenerateTables(unittest.TestCase):
    maxDiff = None

    def test_two_tables(self):
        seq = get_sql_sequence(os.path.join(static_dir, "test_two_tables", "sequence2.tsv"),
                               "test", "A,B,C".split(","), False)
        expected_outputs = [
            ("test_A_B", "select * from test.A a inner join test.B b on a.id = b.id"),
            ("test_A_C", "select * from test.A a inner join test.C b on a.id = b.id"),
            ("test_B_C", "select * from test.B a inner join test.C b on a.id = b.id"),
            ("another_test", "select * from test.test")
        ]
        self.assertEqual(expected_outputs, seq)

    def test_self_match(self):
        seq = get_sql_sequence(os.path.join(static_dir, "test_two_tables", "sequence1.tsv"),
                               "test", "A,B,C".split(","), True)
        print(seq)
        expected_outputs = [
            ("test_A_A", "select * from test.A a inner join test.A b on a.id = b.id"),
            ("test_B_B", "select * from test.B a inner join test.B b on a.id = b.id"),
            ("test_C_C", "select * from test.C a inner join test.C b on a.id = b.id"),
            ("another_test", "select * from test.test")
        ]
        self.assertEqual(expected_outputs, seq)

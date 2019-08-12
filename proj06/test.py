from project import *
import unittest

class TestClass(unittest.TestCase):
    def test_1(self):
        ### Mimir test input goes here (be sure to indent)
        c = Collection()

        docs = [

            {"student": "Josh", "grades": {"hw1": 2, "hw2": 1.5}},

            {"student": "Emily", "grades": {"hw2": 2, "hw3": 1.5}},

            {"student": "Charles", "grades": {}},

            {"student": "Tyler"},

            {"student": "Grant", "grades": {"hw1": 2, "hw3": 1.5}},

            {"student": "Rich", "grades": {"hw2": 2, "hw3": 1}},

        ]

        for doc in docs:
            c.insert(doc)

        assert c.find_all() == docs

        expected = [
            {"student": "Emily", "grades": {"hw2": 2, "hw3": 1.5}},
            {"student": "Rich", "grades": {"hw2": 2, "hw3": 1}},
        ]

        student = c.find({"grades": {"hw2": 2}})

        assert student == expected, student

        ### Mimir test input ends here

if __name__ == "__main__":
    unittest.main()
import unittest
from random_user import get_n, get_random_user_agent


class TestGetRandomUserAgent(unittest.TestCase):
    def test_gen_many(self):
        for _ in range(10000):
            get_random_user_agent()

class TestGetN(unittest.TestCase):

    def test_get_n(self):

        j = get_n(1)
        self.assertEqual(len(j["results"]), 1)

        j = get_n(10)
        self.assertEqual(len(j["results"]), 10)


if __name__ == "__main__":
    unittest.main()


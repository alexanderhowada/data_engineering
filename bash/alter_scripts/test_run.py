import os
import unittest
import subprocess
from copy import deepcopy


TEST_FILE = "testfile"
TEST_SCRIPT = "bash/alter_scripts/run.sh"

def get_file():
    with open(TEST_FILE, "r") as f:
        s = f.read().splitlines()
    return s

class TestRunSH(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.testfile = [
            "asdf", "rewq",
            "123", "333"
        ]

    @classmethod
    def tearDownClass(cls):
        os.remove(TEST_FILE)

    @classmethod
    def setUp(cls):
        with open(TEST_FILE, "w") as f:
            for l in cls.testfile:
                f.write(str(l))
                f.write("\n")

    def test_replace_asdf(self):
        subprocess.run(["bash", TEST_SCRIPT, ".", TEST_FILE, "asdf", "zzzz"])

        correct = deepcopy(self.testfile)
        correct[0] = "zzzz"

        s = get_file()
        self.assertEqual(s, correct)

    def test_replace_az(self):
        subprocess.run(["bash", TEST_SCRIPT, ".", TEST_FILE, "[a-z]+", ""])

        correct = deepcopy(self.testfile)
        correct[0] = ""
        correct[1] = ""

        s = get_file()
        self.assertEqual(s, correct)

    def test_replace_09(self):
        subprocess.run(["bash", TEST_SCRIPT, ".", TEST_FILE, "[0-9]+", ""])

        correct = deepcopy(self.testfile)
        correct[2] = ""
        correct[3] = ""

        s = get_file()
        self.assertEqual(s, correct)


if __name__ == "__main__":
    unittest.main()


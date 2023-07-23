import unittest

from utils.string_utils import normalize_string, camel_to_snake

class TestNormalizeString(unittest.TestCase):

    def test_accents(self):
        accents = "È,É,Ê,Ë,Û,Ù,Ï,Î,À,Â,Ô,è,é,ê,ë,û,ù,ï,î,à,â,ô,Ç,ç,Ã,ã,Õ,õ"
        expected = "E E E E U U I I A A O e e e e u u i i a a o C c A a O o"
        accents2 = "çÇáéíóúýÁÉÍÓÚÝàèìòùÀÈÌÒÙãõñäëïöüÿÄËÏÖÜÃÕÑâêîôûÂÊÎÔÛ"
        expected2 = "cCaeiouyAEIOUYaeiouAEIOUaonaeiouyAEIOUAONaeiouAEIOU"

        self.assertEqual(normalize_string(accents), expected)
        self.assertEqual(normalize_string(accents2), expected2)

    def test_special_characters(self):
        s = "[]!@#$$%&*&%1234"
        expected = '1234'

        self.assertEqual(normalize_string(s), expected)

class TestCamelToSnake(unittest.TestCase):

    def test_simple_case(self):
        s = "CamelCase"
        ss = "camel_case"

        self.assertEqual(camel_to_snake(s), ss)

    def test_multiple_upper(self):
        s = "CAMelCAseCamelCase"
        ss = "camel_case_camel_case"

        self.assertEqual(camel_to_snake(s), ss)

    def test_multiple_upper_number(self):
        s = "CAMel12CAse34Camel56Case111"
        ss = "camel12_case34_camel56_case111"

        self.assertEqual(camel_to_snake(s), ss)

    def test_camel_snake(self):
        s = "CAmel1Case2Camel_Case12_Camel12_Case1"
        ss = "camel1_case2_camel_case12_camel12_case1"

        self.assertEqual(camel_to_snake(s), ss)

    def test_empty_string(self):
        s = ""
        ss = ""

        self.assertEqual(camel_to_snake(s), ss)

    def test_n_underscore(self):

        for i in range(100):
            s = i*"_"
            self.assertEqual(camel_to_snake(s), "")


if __name__ == '__main__':
    unittest.main()


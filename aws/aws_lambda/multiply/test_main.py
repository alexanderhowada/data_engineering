import json
import unittest
from aws.aws_lambda.multiply.main import main


class TestMain(unittest.TestCase):

    def test_1(self):
        event = {
            'array': [1, 2, 3, 4]
        }
        r = json.loads(main(event, None)['body'])

        self.assertEqual(
            r,
            {'s': 10, 'm': 24}
        )

    def test_empty(self):
        event = {
            'array': []
        }
        r = json.loads(main(event, None)['body'])

        self.assertEqual(
            r,
            {'s': None, 'm': None}
        )


if __name__ == '__main__':
    unittest.main()


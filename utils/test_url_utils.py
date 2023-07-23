import unittest
from utils.url_utils import get_json_from_url

class TestUrl(unittest.TestCase):
    def test_valid_url(self):
        url = 'https://example.com/api?json={"foo": "bar", "baz": 42}'
        expected_output = {"foo": "bar", "baz": 42}
        self.assertEqual( get_json_from_url(url), expected_output)

    def test_missing_json_parameter(self):
        url = 'https://example.com/api'
        with self.assertRaises(ValueError):
            get_json_from_url(url)

    def test_empty_json_parameter(self):
        url = 'https://example.com/api?json='
        expected_output = {}
        self.assertEqual(get_json_from_url(url), expected_output)

    def test_invalid_url(self):
        url = 'https://example.com/api?foo='
        with self.assertRaises(ValueError):
            get_json_from_url(url)

    def test_sm_url(self):
        url = "https://aws1-api-default.supermetrics.com/enterprise/v2/query/data/json?json=%7B%22ds_id%22%3A%22DBM%22%2C%22start_date%22%3A%222022-01-01%22%2C%22end_date%22%3A%222022-01-31%22%2C%22fields%22%3A%22account_id%2Caccount%2CcampaignID%2Ccampaign%2CUniqueReachTotalReach%22%2C%22settings%22%3A%7B%22blanks_to_zero%22%3Atrue%2C%22show_all_time_values%22%3Atrue%7D%2C%22max_rows%22%3A10000%2C%22api_key%22%3A%22asdf%22%7D"
        expect = {
            "ds_id": "DBM",
            "start_date": "2022-01-01",
            "end_date": "2022-01-31",
            "fields": "account_id,account,campaignID,campaign,UniqueReachTotalReach",
            "settings": {
                "blanks_to_zero": True,
                "show_all_time_values": True
            },
            "max_rows": 10000,
            "api_key": "asdf"
        }
        self.assertEqual(expect, get_json_from_url(url))

if __name__ == '__main__':
    unittest.main()
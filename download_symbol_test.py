import unittest
import re
import json

class TestDownloadSymbol(unittest.TestCase):

    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)

    def test_download_symbol(self):
        self.crypto = 'BTC'
        self.filename = f'results/{self.crypto}.out'
        self.json_filename = f'results/{self.crypto}.json'
        with open(self.filename, 'r') as file:
            lines = file.readlines()
            with open(self.json_filename, 'a') as json_file:
                for line in lines:
                    reg_exp = re.search(r"\S - \[(\d+), '(\S+)', '(\S+)', '(\S+)', '(\S+)', '(\S+)', (\S+), '(\S+)', (\S+), '(\S+)', '(\S+)', '(\S+)']", line)
                    Candlestick = {
                        'open_time' : int(reg_exp.group(1)),
                        'open': float(reg_exp.group(2)),
                        'high': float(reg_exp.group(3)),
                        'low': float(reg_exp.group(4)),
                        'close': float(reg_exp.group(5)),
                        'volume': float(reg_exp.group(6)),
                        'close_time': int(reg_exp.group(7)),
                        'quote_asset_volume': float(reg_exp.group(8)),
                        'number_of_trades': int(reg_exp.group(9)),
                        'taker_buy_base_asset_volume': float(reg_exp.group(10)),
                        'taker_buy_quote_asset_volume': float(reg_exp.group(11))
                    }
                    json_string = json.dumps(Candlestick)
                    json_file.write(f'{json_string}\n')

if __name__ == '__main__':
    unittest.main()
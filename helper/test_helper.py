# Test suite for helper.py functions

__package__ = 'harvester.helper'
import unittest
from . import helper

class TestHelperFunctions(unittest.TestCase):

    #1 placeholder
    def test_readJSON(self):
        self.assertEqual(1,1)

    #2 placeholder
    def test_writeJSON(self):
        self.assertEqual(1,1)

    # 3) Test lowercase conversion works correctly across multiple object types
    def test_lowercase(self):

        # if type == dict:
        self.assertEqual(helper.lowercase(
                         {'TEST1':'Test1', 'TesT2' : 2, 'TeST3' : ['Test3', 'TEST3', 3]}),
                         {'test1':'test1', 'test2' : 2, 'test3' : ['test3', 'test3', 3]})

        # elif if type == list,set,tuple
        self.assertEqual(helper.lowercase(['Test', 'TEST', 1]), ['test', 'test', 1])
        self.assertEqual(helper.lowercase(set(['Test', 'TEST', 2, 'TeSt', 'TEST'])), set(['test', 2]))
        self.assertEqual(helper.lowercase(('Test', 'TEST', 3)), ('test', 'test', 3))

        # elif if type ==  str
        self.assertEqual(helper.lowercase('TeST1'), 'test1')

        # else
        self.assertEqual(1,1)

    #4 placeholder
    def test_check_dictionary(self):
        self.assertEqual(1,1)

    #5 placeholder
    def test_dict_to_json(self):
        self.assertEqual(1,1)

    #6 placeholder
    def test_calculate_checksum(self):
        self.assertEqual(1,1)

if __name__ == "__main__":
    unittest.main()

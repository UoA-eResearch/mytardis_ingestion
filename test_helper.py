# Test suite for helper.py functions

import os
import unittest
import tempfile
from helper import helper

# generic "JSON-style" dictionary for JSON-function testing
dictionary = { "office":
    {"medical": [
      { "room-number": 100,
        "use": "reception",
        "sq-ft": 50,
        "price": 75
      },
      { "room-number": 101,
        "use": "waiting",
        "sq-ft": 250,
        "price": 75
      },
      { "room-number": 102,
        "use": "examination",
        "sq-ft": 125,
        "price": 150
      },
      { "room-number": 103,
        "use": "examination",
        "sq-ft": 125,
        "price": 150
      },
      { "room-number": 104,
        "use": "office",
        "sq-ft": 150,
        "price": 100
      }
    ],
    "parking": {
      "location": "premium",
      "style": "covered",
      "price": 750
    }
  }
}


class TestHelperFunctions(unittest.TestCase):
    """
    Unittest for helper functions in helper.helper
    TODO - EXPAND THIS
    """

    def test_readJSON(self):
        """
        1) Test if JSON can be read correctly from file to python dictionary
           TODO - add filenotfound test - add exception test
        """
        testread = helper.readJSON('./test_data/test_data_read.json')
        self.assertEqual(testread, dictionary)


    def test_writeJSON(self):
        """
        2) Test if JSON can be written correctly from python dictionary to file
           TODO - add Exception test for "cant write file"
        """
        output_path = tempfile.mkstemp()[1]
        try:
            # write dictionary to temporary json file
            testwrite = helper.writeJSON(dictionary,output_path)
            # store written file results
            with open(output_path) as f:
                contents = f.read()
        finally:
            # tidy up temporary json file file, results stored in memory above
            os.remove(output_path)
        # read in the expected result from test json file (not using readJSON)
        with open('./test_data/test_data_read.json') as f:
            answer = f.read()
        # check that writeJSON returns true, and that output is correct
        self.assertTrue(testwrite)
        self.assertEqual(contents,answer)


    def test_lowercase(self):
        """
        3) Test if lowercase conversion works correctly across multiple object types
        """
        # test dictionary handling
        self.assertEqual(helper.lowercase({'TEST1':'Test1', 'TesT2' : 2, 'TeST3' : ['Test3', 'TEST3', 3]}),
                                          {'test1':'test1', 'test2' : 2, 'test3' : ['test3', 'test3', 3]})
        # test list,set,tuple handling
        self.assertEqual(helper.lowercase(['Test', 'TEST', 1]),
                                          ['test', 'test', 1])
        self.assertEqual(helper.lowercase(set(['Test', 'TEST', 2, 'TeSt', 'TEST'])),
                                          set(['test', 2]))
        self.assertEqual(helper.lowercase(('Test', 'TEST', 3)),
                                          ('test', 'test', 3))
        # test string handling
        self.assertEqual(helper.lowercase('TeST1'), 'test1')
        # test "else" handling (no need to test more complex objects?)
        self.assertEqual(1,1)


    #4 placeholder
    def test_check_dictionary(self):
        self.assertEqual(1,1)


    #5 placeholder
    def test_dict_to_json(self):
        self.assertEqual(1,1)


    #6 placeholder
    def test_calculate_etag(self):
        self.assertEqual(1,1)

if __name__ == "__main__":
    unittest.main()

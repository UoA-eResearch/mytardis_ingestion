# Module testing for myTardis ingestion functions

import os
import unittest
import tempfile
import datetime
from mock import patch, call

from helper import helper
from mailserver import mailserver

# generic "JSON-style" dictionary for JSON-function testing
helper_dictionary = { "office":
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


class HelperFunctions(unittest.TestCase):
    """
    Unittests for helper functions in helper.helper

    Loggers have been funneled into Mock objects, but can be retrieved by declaring
    an instance of the Mock object and using the .mock_calls function to access the
    structures therein.
    """

    @patch("helper.helper.logger")
    def test_readJSON(self, mock_logger):
        """
        1) Test if JSON can be read correctly from file to python dictionary
        """
        # test that json file can be correctly loaded into dictionary
        testread = helper.readJSON('./test_data/test_data_read.json')
        self.assertEqual(testread, helper_dictionary)
        # test that FileNotFoundError exception is raised as expected
        with self.assertRaises(FileNotFoundError):
            testread = helper.readJSON('./test_data/bad_filename')
        # test that any Exception is raised as expected
        with self.assertRaises(Exception):
            testread = helper.readJSON(PermissionError)


    @patch("helper.helper.logger")
    def test_writeJSON(self, mock_logger):
        """
        2) Test if JSON can be written correctly from python dictionary to file
        """
        # define temporary file
        output_path = tempfile.mkstemp()[1]
        try:
            # write dictionary to temporary json file
            testwrite = helper.writeJSON(helper_dictionary,output_path)
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
        # test that any Exception is raised as expected
        with self.assertRaises(Exception):
            testwrite = helper.writeJSON(helper_dictionary,PermissionError)


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


    def test_check_dictionary(self):
        """
        4) Test if dictionary sanity check correctly finds keys/returns missing keys
        """
        # add additional keys to test dictionary
        longer_dictionary = {**helper_dictionary, **{'office2':2,'office3':3}}
        # test that check_dictionary() finds all keys and returns True
        required_keys = ['office','office2','office3']
        result = helper.check_dictionary(longer_dictionary, required_keys)
        self.assertEqual(result,(True,[]))
        # test that check_dictionary() does not find keys, and returns (False, [missing_keys])
        required_keys = ['office4','office2','office3']
        result = helper.check_dictionary(longer_dictionary, required_keys)
        self.assertEqual(result,(False,['office4']))


    def test_dict_to_json(self):
        """
        5) Test if datetime objects in dictionaries correctly transformed to strings
           when writing dictionary to json string
        """
        # test that TypeError is raised unless a dictionary is passed to function
        with self.assertRaises(TypeError):
            result = helper.dict_to_json('NOT_A_DICT')
        with self.assertRaises(TypeError):
            result =  helper.dict_to_json(1)
        with self.assertRaises(TypeError):
            result =  helper.dict_to_json(('NOT_A_DICT', 2))
        with self.assertRaises(TypeError):
            result =  helper.dict_to_json(['NOT_A_DICT', 'NOT_A_DICT2'])

        # create test dictionary with datetime.date and datetime.datetime objects
        time_dict = {'test_date': datetime.date(2020,1,31),
                     'test_datetime': datetime.datetime(2020,1,31,23,59,59,10)}
        # test that datetime objects within dictionary are correctly transformed to string
        # Expected result: dictionary written as string, with datetime objects converted to string equivalents
        # i.e. '-' yyyy-mm-ddThh:mm:ss:milliseconds
        result =  helper.dict_to_json(time_dict)
        self.assertEqual(result, '{"test_date": "2020-01-31", "test_datetime": "2020-01-31T23:59:59.000010"}')


    def test_calculate_etag(self):
        """
        6) Test if calculate_etag correctly processes a file when read in:
           A) multiple chunks
           B) a sungle chunk
           C) or if the file is empty
        """
        # create temporary files for testing
        output_path = tempfile.mkstemp()[1]
        output_path_empty = tempfile.mkstemp()[1]
        try:
            # store written file results
            with open(output_path, 'wb+') as f:
                f.write(b'123456789abcdef')
            # test that calculate_etag retrieves and encodes file when chunks > 1
            # each chunk is 'md5'd => digested => chuncks combined together => then re-'md5'd
            # before hexdigesting result and appending number of chunks to string
            self.assertEqual(helper.calculate_etag(output_path,5),'e3b67ed03dde49ef88061cfcfa5218ca'+'-3')
            # test that calculate_etag retrieves file and encodes when chunks = 1
            self.assertEqual(helper.calculate_etag(output_path,15),'649f312aaffac7561ae81f8f9c334f14')
            # test that an empty file is read in as str("") when chunks = 0
            self.assertEqual(helper.calculate_etag(output_path_empty,15),'""')
        finally:
            # tidy up temporary json file file, results stored in memory above
            os.remove(output_path)
            os.remove(output_path_empty)




mailserver_dictionary = { 'server': 'test_server',
                          'port' : '465',
                          'username' : 'test_user',
                          'password' : 'test_password',
                          'receivers' : ['test_receiver']
                         }

class MailServerFunctions(unittest.TestCase):
    """
    Unittests for Mailserver functions in mailserver.mailserver
    """

    @patch("smtplib.SMTP_SSL")
    def test_MailHandler_single_send(self,mock_SMTP_SSL):
        """
        1) Tests that a email is sent properly to a single recipient, from a specified sender
        """

        # initialise mailhandler class
        mail_test = mailserver.MailHandler(mailserver_dictionary)
        # define a test subject, test message, and specified sender
        subject = 'THIS IS A TEST'
        message = 'Cryptic warning or pleasant upload notification'
        test_sender = 'test_sender'

        # send email from and to 'mock' server (in the form of a Mock class)
        mail_test.send_message(subject,message,sender=test_sender)
        # collect results from mock class
        instance = mock_SMTP_SSL
        # check that function was called exactly once (no one wants spam)
        self.assertEqual(instance.call_count, 1)
        # check that email went through correct server and port
        self.assertEqual((instance.mock_calls[0][2]['host'],
                          instance.mock_calls[0][2]['port']),
                         (mailserver_dictionary['server'],
                          mailserver_dictionary['port']))
        # check that user and password handed across correctly
        self.assertEqual(instance.mock_calls[2][1],
                         (mailserver_dictionary['username'],
                          mailserver_dictionary['password']))
        # check that sender and receiver set correctly
        self.assertEqual((instance.mock_calls[3][1][0],
                          instance.mock_calls[3][1][1]),
                         (test_sender, ', '.join(mailserver_dictionary['receivers'])))
        # check the contents of the email are correct
        self.assertTrue( ('Subject: ' + subject) in instance.mock_calls[3][1][2])
        self.assertTrue( ('From: ' + test_sender) in instance.mock_calls[3][1][2])
        self.assertTrue( ('To: ' + ', '.join(mailserver_dictionary['receivers'])) in instance.mock_calls[3][1][2])
        self.assertTrue( (message) in instance.mock_calls[3][1][2])


    @patch("smtplib.SMTP_SSL")
    def test_MailHandler_multi_nosend(self,mock_SMTP_SSL):
        """
        2) Tests that a email is sent properly to multiple recipients, from the default sender
        """

        # add additional recipients to the dictionary
        mailserver_dictionary['receivers'].extend(['test_receiver2','test_receiver3'])
        # initialise mailhandler class
        mail_test = mailserver.MailHandler(mailserver_dictionary)
        # define a test subject and test message
        subject = 'THIS IS A TEST'
        message = 'Cryptic warning or pleasant upload notification'

        # send email from and to 'mock' server (in the form of a Mock class)
        mail_test.send_message(subject,message)
        # collect results from mock class
        instance = mock_SMTP_SSL
        # check that function was called exactly once (no one wants spam)
        self.assertEqual(instance.call_count, 1)
        # check that email went through correct server and port
        self.assertEqual((instance.mock_calls[0][2]['host'],
                          instance.mock_calls[0][2]['port']),
                         (mailserver_dictionary['server'],
                          mailserver_dictionary['port']))
        # check that user and password handed across correctly
        self.assertEqual(instance.mock_calls[2][1],
                         (mailserver_dictionary['username'],
                          mailserver_dictionary['password']))
        # check that sender and receiver set correctly
        self.assertEqual((instance.mock_calls[3][1][0],
                          instance.mock_calls[3][1][1]),
                         ('myTardis_do_not_reply@auckland.ac.nz', ', '.join(mailserver_dictionary['receivers'])))
        # check the contents of the email are correct
        self.assertTrue( ('Subject: ' + subject) in instance.mock_calls[3][1][2])
        self.assertTrue( ('From: ' + 'myTardis_do_not_reply@auckland.ac.nz') in instance.mock_calls[3][1][2])
        self.assertTrue( ('To: ' + ', '.join(mailserver_dictionary['receivers'])) in instance.mock_calls[3][1][2])
        self.assertTrue( (message) in instance.mock_calls[3][1][2])

if __name__ == "__main__":
    unittest.main()
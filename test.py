import unittest
from unittest.mock import MagicMock
from Sql_python_Assignment import *

# To test connecction

class test(unittest.TestCase):
    def test_connection(self):
        mock_connect = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value=2
        mock_connect.cursor.return_value = mock_cursor

        result=connect()

        self.assertEqual(result,2)


if __name__ == '__main__':
    unittest.main()

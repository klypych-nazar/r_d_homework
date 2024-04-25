"""
Tests for main.py
"""
from unittest import TestCase, mock

from lesson_02.job_1 import main


class MainFunctionTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()

    @mock.patch('lesson_02.job_1.main.save_sales_to_local_disk')
    def test_return_400_date_param_missed(self, save_sales_to_local_disk_mock: mock.MagicMock):
        """
        Test that a 400 Bad Request status code is returned when the 'date' parameter
        is not provided in the POST request.
        """
        # Arrange
        fake_raw_dir = '/foo/bar/'

        # Act
        response = self.client.post(
            '/',
            json={
                'raw_dir': fake_raw_dir,  # 'date' is missing
            },
        )

        # Assert
        self.assertEqual(400, response.status_code, "The response status code should be 400 when 'date' is missing.")
        self.assertIn("date parameter missed", response.json['message'],
                      "The error message should indicate missing 'date' parameter.")

    @mock.patch('lesson_02.job_1.main.save_sales_to_local_disk')
    def test_return_400_raw_dir_param_missed(self, save_sales_to_local_disk_mock: mock.MagicMock):
        """
        Test that a 400 Bad Request status code is returned when the 'raw_dir' parameter
        is not provided in the POST request.
        """
        # Arrange
        fake_date = '1970-01-01'

        # Act
        response = self.client.post(
            '/',
            json={
                'date': fake_date,  # 'raw_dir' is missing
            },
        )

        # Assert
        self.assertEqual(400, response.status_code, "The response status code should be 400 when 'raw_dir' is missing.")
        self.assertIn("raw_dir parameter missed", response.json['message'],
                      "The error message should indicate missing 'raw_dir' parameter.")

    @mock.patch('lesson_02.job_1.main.save_sales_to_local_disk')
    def test_save_sales_to_local_disk(self, save_sales_to_local_disk_mock: mock.MagicMock):
        """
        Test whether save_sales_to_local_disk is called with the correct parameters
        when a POST request is made to the endpoint with the correct JSON body.
        """
        fake_date = '1970-01-01'
        fake_raw_dir = '/foo/bar/'

        response = self.client.post(
            '/',
            json={
                'date': fake_date,
                'raw_dir': fake_raw_dir,
            },
        )

        self.assertEqual(201, response.status_code,
                         "The response status code should be 201 when all parameters are provided.")
        self.assertIn("Data retrieved successfully from API", response.json['message'],
                      "The success message should be in the response.")
        save_sales_to_local_disk_mock.assert_called_once_with(
            date=fake_date,
            raw_dir=fake_raw_dir,
        ), "save_sales_to_local_disk should be called once with the correct parameters."

    @mock.patch('lesson_02.job_1.main.save_sales_to_local_disk')
    def test_return_201_when_all_is_ok(self, save_sales_to_local_disk_mock: mock.MagicMock):
        """
        Test that the endpoint returns a 201 Created status code when the 'date'
        and 'raw_dir' parameters are provided and the save_sales_to_local_disk
        function does not raise any exceptions.
        """
        correct_date = '2022-08-09'
        correct_raw_dir = '/path/to/my_dir/raw/sales/2022-08-09'

        response = self.client.post(
            '/',
            json={
                'date': correct_date,
                'raw_dir': correct_raw_dir,
            },
        )

        self.assertEqual(201, response.status_code)
        save_sales_to_local_disk_mock.assert_called_once_with(date=correct_date, raw_dir=correct_raw_dir)
        self.assertIn("Data retrieved successfully from API", response.json['message'])

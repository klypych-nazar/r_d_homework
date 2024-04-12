"""
Tests sales_api.py module.
"""
from unittest import TestCase, mock
from lesson_02.job_1.dal.sales_api import get_sales


class GetSalesTestCase(TestCase):
    """
    Test sales_api.get_sales function.
    """

    @mock.patch("lesson_02.job_1.dal.sales_api.httpx.Client.get")
    def test_get_sales_success_1(self, mock_get):
        # mocking successful API responses for two pages
        mock_get.side_effect = [
            mock.Mock(status_code=200, json=lambda: [i for i in range(100)]),
            mock.Mock(status_code=200, json=lambda: [i for i in range(30)]),
        ]

        expected_length = 130

        data = get_sales('2022-08-10')
        self.assertEqual(len(data), expected_length)

    @mock.patch("lesson_02.job_1.dal.sales_api.httpx.Client.get")
    def test_get_sales_success_2(self, mock_get):
        # mocking successful API responses for two pages
        mock_get.side_effect = [
            mock.Mock(status_code=200, json=lambda: [i for i in range(100)]),
            mock.Mock(status_code=200, json=lambda: [i for i in range(100)]),
            mock.Mock(status_code=404, json=lambda: []),
        ]

        expected_length = 200

        data = get_sales('2022-08-10')
        self.assertEqual(len(data), expected_length)

    @mock.patch("lesson_02.job_1.dal.sales_api.httpx.Client.get")
    def test_get_sales_404(self, mock_get):
        # Mock a 404 response to simulate the API not finding any data for the given date
        mock_get.return_value = mock.Mock(status_code=404)

        data = get_sales('2022-08-10')
        self.assertEqual(data, [])  # Expecting an empty list when the API returns 404


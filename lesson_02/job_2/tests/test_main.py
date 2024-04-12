import unittest
from unittest import mock
from lesson_02.job_2.dal import local_disk, transform
from lesson_02.job_2.bll.staging import raw_to_stage


class TestRawToStage(unittest.TestCase):
    def setUp(self):
        self.raw_dir = '/fake/raw/dir'
        self.stg_dir = '/fake/stage/dir'
        self.json_content = [{'client': 'Alice', 'purchase_date': '2021-01-01', 'product': 'Widget', 'price': 100}]
        self.file_name = '/fake/stage/dir/stage.avro'

    @mock.patch('lesson_02.job_2.dal.transform.json_to_avro')
    @mock.patch('lesson_02.job_2.dal.local_disk.add_file_name_to_path', return_value='/fake/stage/dir/stage.avro')
    @mock.patch('lesson_02.job_2.dal.local_disk.manage_directories')
    @mock.patch('lesson_02.job_2.dal.local_disk.read_from_disk', return_value=[{'client': 'Alice', 'purchase_date': '2021-01-01', 'product': 'Widget', 'price': 100}])
    def test_successful_data_transformation(self, mock_read, mock_manage, mock_add_file_name, mock_json_to_avro):
        """
         Test that raw_to_stage processes and transforms data correctly under normal conditions,
         confirming that all mocks are called with expected arguments and confirming that data
         flows through the transformation pipeline as expected.
         """
        raw_to_stage(self.raw_dir, self.stg_dir)
        mock_read.assert_called_once_with(raw_dir=self.raw_dir)
        mock_manage.assert_called_once_with(self.stg_dir)
        mock_add_file_name.assert_called_once_with(self.stg_dir, file_type='avro')
        mock_json_to_avro.assert_called_once_with(self.json_content, file_name=self.file_name)

    @mock.patch('lesson_02.job_2.dal.transform.json_to_avro')
    @mock.patch('lesson_02.job_2.dal.local_disk.add_file_name_to_path', return_value='/fake/stage/dir/stage.avro')
    @mock.patch('lesson_02.job_2.dal.local_disk.manage_directories')
    @mock.patch('lesson_02.job_2.dal.local_disk.read_from_disk', side_effect=FileNotFoundError)
    def test_file_not_found_exception(self, mock_read, mock_manage, mock_add_file_name, mock_json_to_avro):
        """
        Test that raw_to_stage properly handles a FileNotFoundError,
        ensuring that subsequent functions are not called after the exception is raised.
        """
        with self.assertRaises(FileNotFoundError):
            raw_to_stage(self.raw_dir, self.stg_dir)
        mock_read.assert_called_once_with(raw_dir=self.raw_dir)
        mock_manage.assert_not_called()
        mock_add_file_name.assert_not_called()
        mock_json_to_avro.assert_not_called()

    @mock.patch('lesson_02.job_2.dal.transform.json_to_avro')
    @mock.patch('lesson_02.job_2.dal.local_disk.add_file_name_to_path', return_value='/fake/stage/dir/stage.avro')
    @mock.patch('lesson_02.job_2.dal.local_disk.manage_directories')
    @mock.patch('lesson_02.job_2.dal.local_disk.read_from_disk', return_value=[])
    def test_empty_data_handling(self, mock_read, mock_manage, mock_add_file_name, mock_json_to_avro):
        """
        Test the handling of empty data by raw_to_stage,
        ensuring that all processes handle the empty data scenario gracefully.
        """
        raw_to_stage(self.raw_dir, self.stg_dir)
        mock_read.assert_called_once_with(raw_dir=self.raw_dir)
        mock_manage.assert_called_once_with(self.stg_dir)
        mock_add_file_name.assert_called_once_with(self.stg_dir, file_type='avro')
        mock_json_to_avro.assert_called_once_with([], file_name=self.file_name)

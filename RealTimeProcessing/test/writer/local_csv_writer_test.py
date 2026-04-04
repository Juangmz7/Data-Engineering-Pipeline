import pytest
import shutil
from unittest.mock import MagicMock, patch

from RealTimeProcessing.src.writer.local_csv_writer import LocalCsvWriter


@pytest.fixture
def mock_logger():
    with patch("RealTimeProcessing.src.writer.local_csv_writer.get_pipeline_logger") as mock:
        yield mock.return_value


@pytest.fixture
def mock_id_gen():
    with patch(
        "RealTimeProcessing.src.writer.local_csv_writer.IdGenerator.generate",
        return_value="test-local-uuid",
    ):
        yield


@pytest.fixture
def writer(mock_logger, mock_id_gen):
    return LocalCsvWriter(correlation_id="test-correlation-id")


class TestLocalCsvWriter:
    def test_write_success_with_directory_creation(self, writer, mock_logger):
        with patch("RealTimeProcessing.src.writer.local_csv_writer.Path") as mock_path_cls, patch(
            "RealTimeProcessing.src.writer.local_csv_writer.shutil.copy2"
        ) as mock_copy:
            mock_src = MagicMock()
            mock_src.exists.return_value = True

            mock_dst = MagicMock()
            mock_dst.parent.exists.return_value = False

            mock_path_cls.side_effect = [mock_src, mock_dst]

            writer.write("source/data.csv", "target/subdir/data.csv")

        mock_dst.parent.mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_copy.assert_called_once_with(mock_src, mock_dst)
        mock_logger.info.assert_any_call("Data successfully written to local disk.")

    def test_write_raises_file_not_found_when_source_missing(self, writer, mock_logger):
        with patch("RealTimeProcessing.src.writer.local_csv_writer.Path") as mock_path_cls, patch(
            "RealTimeProcessing.src.writer.local_csv_writer.shutil.copy2"
        ) as mock_copy:
            mock_src = MagicMock()
            mock_src.exists.return_value = False

            mock_dst = MagicMock()
            mock_dst.parent.exists.return_value = True

            mock_path_cls.side_effect = [mock_src, mock_dst]

            with pytest.raises(FileNotFoundError, match="Source file not found"):
                writer.write("missing.csv", "dest.csv")

        mock_logger.error.assert_any_call(f"Source file not found: {mock_src}")
        mock_logger.error.assert_any_call(f"File not found during local copy to {mock_dst}: Source file not found: {mock_src}")
        mock_copy.assert_not_called()

    def test_write_handles_permission_error(self, writer, mock_logger):
        with patch("RealTimeProcessing.src.writer.local_csv_writer.Path") as mock_path_cls, patch(
            "RealTimeProcessing.src.writer.local_csv_writer.shutil.copy2",
            side_effect=PermissionError("Access denied"),
        ):
            mock_src = MagicMock()
            mock_src.exists.return_value = True

            mock_dst = MagicMock()
            mock_dst.parent.exists.return_value = True

            mock_path_cls.side_effect = [mock_src, mock_dst]

            with pytest.raises(PermissionError, match="Access denied"):
                writer.write("src.csv", "dst.csv")

        mock_logger.error.assert_any_call(f"Permission denied writing to {mock_dst}: Access denied")

    def test_write_handles_same_file_error(self, writer, mock_logger):
        with patch("RealTimeProcessing.src.writer.local_csv_writer.Path") as mock_path_cls, patch(
            "RealTimeProcessing.src.writer.local_csv_writer.shutil.copy2",
            side_effect=shutil.SameFileError("Identical paths"),
        ):
            mock_src = MagicMock()
            mock_src.exists.return_value = True

            mock_dst = MagicMock()
            mock_dst.parent.exists.return_value = True

            mock_path_cls.side_effect = [mock_src, mock_dst]

            with pytest.raises(shutil.SameFileError, match="Identical paths"):
                writer.write("src.csv", "src.csv")

        mock_logger.error.assert_any_call("Source and destination paths are identical: Identical paths")

    def test_write_handles_os_error(self, writer, mock_logger):
        with patch("RealTimeProcessing.src.writer.local_csv_writer.Path") as mock_path_cls, patch(
            "RealTimeProcessing.src.writer.local_csv_writer.shutil.copy2",
            side_effect=OSError("No space left"),
        ):
            mock_src = MagicMock()
            mock_src.exists.return_value = True

            mock_dst = MagicMock()
            mock_dst.parent.exists.return_value = True

            mock_path_cls.side_effect = [mock_src, mock_dst]

            with pytest.raises(OSError, match="No space left"):
                writer.write("src.csv", "dst.csv")

        mock_logger.error.assert_any_call(f"OS error during local copy to {mock_dst}: No space left")

    def test_write_handles_unexpected_exceptions(self, writer, mock_logger):
        with patch(
            "RealTimeProcessing.src.writer.local_csv_writer.Path",
            side_effect=Exception("Unexpected crash"),
        ):
            with pytest.raises(Exception, match="Unexpected crash"):
                writer.write("src.csv", "dst.csv")

        mock_logger.critical.assert_called_once()
        args, kwargs = mock_logger.critical.call_args
        assert args[0] == "Unexpected runtime error during local copy: Unexpected crash"
        assert kwargs.get("exc_info") is True

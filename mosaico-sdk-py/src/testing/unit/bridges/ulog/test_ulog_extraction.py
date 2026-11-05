from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from mosaicolabs.bridges.topic_status import TopicStatus
from mosaicolabs.bridges.ulog import (
    MosaicoToULogLoader,
    ULogExtractorConfig,
    ULogFileWriter,
    ULogLoader,
    ULogSequenceExtractor,
)
from mosaicolabs.enum.serialization_format import SerializationFormat
from mosaicolabs.models.core import Message

ULOG_DIR = Path(__file__).resolve().parents[3] / "data" / "ulog"
ULOG_FILES = sorted(ULOG_DIR.glob("*.ulg"))

ULOG_METADATA = {
    "message_name": "baro",
    "multi_id": 0,
    "format_def": "",
    "nested_formats": {},
}


def _handler(ulog_metadata: dict) -> SimpleNamespace:
    """The `TopicHandler` attributes read by `MosaicoToULogLoader._resolve_topic`."""
    return SimpleNamespace(
        user_metadata={"_ulog_": ulog_metadata},
        ontology_tag="baro",
        ontology_schema=pa.struct([("p", pa.float32())]),
        serialization_format=SerializationFormat.Default,
    )


# --- Loader ---


def test_resolve_topic():
    """A valid `_ulog_` block gives an adapter, a missing or wrong one is rejected."""
    loader = MosaicoToULogLoader(None, "flight")

    resolution = loader._resolve_topic(_handler(ULOG_METADATA))
    assert resolution.is_accepted
    assert resolution.native_msg_type == "baro"

    for malformed in ({}, {**ULOG_METADATA, "multi_id": "0"}):
        resolution = loader._resolve_topic(_handler(malformed))
        assert resolution.status is TopicStatus.MALFORMED_METADATA


# --- Writer ---


@pytest.mark.parametrize("path", ULOG_FILES, ids=lambda p: p.stem)
def test_rows_reach_the_writer_as_native_records(path, tmp_path):
    """Every row of every topic goes through `Message` and `to_native` into the writer."""
    writer = ULogFileWriter(tmp_path / "flight", {})
    with ULogLoader(path) as loader:
        for msg, exc in loader:
            assert exc is None
            adapter = loader.resolve_adapter(msg.topic_name)
            ontology = adapter.ontology_data_type()
            metadata = loader.topic_schema_metadata(msg.topic_name)
            msg_id = writer.register_or_get_msg_id(metadata)
            rows = msg.data.to_pylist()
            for row in rows:
                message = Message._decode(tag_or_type=ontology, **row)
                writer.write(msg_id, adapter.to_native(message))

            expected = [
                {"timestamp": row.pop("timestamp_ns") // 1000, **row} for row in rows
            ]
            # repr, since NaN values never compare equal
            assert repr(writer._topics[msg_id][1]) == repr(expected)
            assert writer.register_or_get_msg_id(metadata) == msg_id

        # One id per topic, the `/ulog/*` ones included
        assert len(writer._topics) == len(loader.topics)


def test_finish_runs_only_after_a_clean_exit(tmp_path):
    """`finish` is left to the file writer, and an interrupted extraction never reaches it."""
    with pytest.raises(NotImplementedError, match="flight.ulg"):
        with ULogFileWriter(tmp_path / "flight", {}):
            pass

    with pytest.raises(KeyboardInterrupt):
        with ULogFileWriter(tmp_path / "flight", {}):
            raise KeyboardInterrupt


# --- Extractor ---


def test_failing_topic_is_skipped(tmp_path):
    """A topic that cannot be encoded is dropped once, and the bars keep moving."""
    extractor = ULogSequenceExtractor(
        ULogExtractorConfig(saving_path=tmp_path, sequence_name="flight")
    )
    writer, ms_loader, ui = MagicMock(), MagicMock(), MagicMock()
    to_native = ms_loader.resolve_adapter.return_value.to_native

    to_native.return_value = {"timestamp": 1}
    extractor._process_message(writer, ms_loader, "/baro/0", MagicMock(), ui)
    writer.write.assert_called_once_with(
        writer.register_or_get_msg_id.return_value, {"timestamp": 1}
    )
    ui.advance_all.assert_called_once_with("/baro/0")

    to_native.side_effect = ValueError("bad row")
    for _ in range(2):
        extractor._process_message(writer, ms_loader, "/baro/0", MagicMock(), ui)

    assert extractor.ignored_topics == {"/baro/0"}
    assert to_native.call_count == 2  # the message after the failure is not encoded
    assert writer.write.call_count == 1
    assert ui.advance_all.call_count == 1
    assert ui.advance_global.call_count == 2

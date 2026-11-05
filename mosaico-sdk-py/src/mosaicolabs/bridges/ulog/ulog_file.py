"""
ULog file writer: the last step of the extraction (Mosaico -> `.ulg`).

Same role and method names as `MCAPFileWriter` (bridges/mcap/mcap_file.py), so the extractor
does not depend on how the file is written: only `ULogFileWriter.finish` does.
"""

from pathlib import Path
from typing import Dict, List, Tuple

from .adapter_base import ULogSchemaMetadata


class ULogFileWriter:
    """
    Collects the records of a sequence, to write them as `<path>/<path.name>.ulg`.

    Native rows (see `UnmodeledAdapter.to_native`) are kept per topic, in the order they
    are streamed, next to the `_ulog_` metadata of the topic.
    """

    def __init__(self, path: Path, sequence_metadata: dict):
        """
        Args:
            path (Path): The output directory, already validated by `_prepare_output_path()`.
            sequence_metadata (dict): The metadata of the sequence, which holds the file-level
                ULog records (see `ULogLoader.sequence_metadata`).
        """
        self._file_path = path / (path.name + ".ulg")
        self._sequence_metadata = sequence_metadata
        self._msg_ids: Dict[Tuple[str, int], int] = {}
        # ponytail: the whole sequence stays in memory, as pyulog does with a file; a
        # streaming writer would not need to keep the rows
        self._topics: List[Tuple[ULogSchemaMetadata, List[dict]]] = []

    def register_or_get_msg_id(self, ulog_metadata: ULogSchemaMetadata) -> int:
        """
        Registers the topic described by `ulog_metadata`, or returns its id if already known.

        Args:
            ulog_metadata (ULogSchemaMetadata): The `_ulog_` metadata of the topic.

        Returns:
            int: The id of the topic, one per `(format, multi_id)` pair.
        """
        key = (ulog_metadata.fields["message_name"], ulog_metadata.fields["multi_id"])
        if key not in self._msg_ids:
            self._msg_ids[key] = len(self._topics)
            self._topics.append((ulog_metadata, []))
        return self._msg_ids[key]

    def write(self, msg_id: int, row: dict):
        """Adds a native row to the topic registered as `msg_id`."""
        self._topics[msg_id][1].append(row)

    def finish(self):
        """
        Writes the collected records to the file.

        The only step tied to the library that writes the file. The ULog records come from:

        - `F`, `A`: `format_def`, `nested_formats` and `multi_id` of the `_ulog_` metadata;
        - `I`, `M`, `P`, `Q`: the `ulog_*` keys of the sequence metadata;
        - `D`: the rows of the data topics;
        - `L`, `C`, `O` and the `P` changed in flight: the rows of the `/ulog/*` topics, the
          ones with an empty `format_def`;
        - `S`, `B`: generated when writing.

        Raises:
            NotImplementedError: Until the library is chosen.
        """
        raise NotImplementedError(
            f"Cannot write '{self._file_path}': the ULog file writer is not available yet."
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # Nothing is open until `finish`: an interrupted extraction writes no file
        if exc_type is None:
            self.finish()

"""
ULogSequenceExtractor: extracts a Mosaico sequence and writes it as a `.ulg` file.

The ULog specialization of `SequenceExtractor`, same structure as `MCAPSequenceExtractor`
(bridges/mcap/sequence_extractor.py). The base class drives the pipeline, this module
supplies the ULog half of it:

1. Stream the sequence, optionally filtered by topic or time window (in nanoseconds),
   through `MosaicoToULogLoader`.
2. Convert each message to its native ULog record with `to_native()`.
3. Hand the records to `ULogFileWriter`.

Typical usage as a script:
    $ python -m mosaicolabs.bridges.ulog.sequence_extractor flight_01 --ulog_path ./exports

Typical usage as a library:
    config = ULogExtractorConfig(saving_path=Path("exports"), sequence_name="flight_01")
    ULogSequenceExtractor(config).run()
"""

import argparse
from dataclasses import dataclass
from pathlib import Path

from mosaicolabs import Message, MosaicoClient

from ..base_sequence_extractor import (
    ExtractorConfig,
    SequenceExtractor,
    _add_common_arguments,
    _common_config_kwargs,
)
from ..ui import ProgressManager
from .loader import MosaicoToULogLoader
from .ulog_file import ULogFileWriter


@dataclass
class ULogExtractorConfig(ExtractorConfig):
    """Configuration for `ULogSequenceExtractor`, see `ExtractorConfig` for the settings."""


class ULogSequenceExtractor(SequenceExtractor):
    """
    Orchestrates the extraction of a Mosaico sequence into a `.ulg` file.

    `run`, the dry run and the output path handling come from `SequenceExtractor`. Until
    `ULogFileWriter.finish` is implemented only `dry_run` gives a result: a real run streams
    the sequence and then raises `NotImplementedError`, and `overwrite` has already cleared
    the output path by then.
    """

    _ms_loader: MosaicoToULogLoader

    def _open_mosaicoloader(self, mclient: MosaicoClient) -> MosaicoToULogLoader:
        # Kept for `_open_writer`, which only receives the output path
        self._ms_loader = MosaicoToULogLoader(
            mclient,
            self.cfg.sequence_name,
            self.cfg.topics,
            self.cfg.start_timestamp_ns,
            self.cfg.end_timestamp_ns,
        )
        return self._ms_loader

    def _open_writer(self, path: Path) -> ULogFileWriter:
        return ULogFileWriter(path, self._ms_loader.sequence_metadata())

    def _process_message(
        self,
        writer: ULogFileWriter,
        ms_loader: MosaicoToULogLoader,
        t_name: str,
        ms_msg: Message,
        ui: ProgressManager,
    ):
        """
        Encodes a single Mosaico message and hands it to the writer.

        Same steps as `MCAPSequenceExtractor._process_message`: a failure never propagates,
        the topic is added to `ignored_topics` and reported in the progress UI.
        """
        if t_name in self.ignored_topics:
            ui.advance_global()
            return

        try:
            row = ms_loader.resolve_adapter(t_name).to_native(ms_msg)
            writer.write(
                writer.register_or_get_msg_id(ms_loader.resolve_metadata(t_name)), row
            )
        except Exception as e:
            return self._handle_encoding_error(
                t_name,
                ui,
                f"Could not write topic '{t_name}' to ulog. Skipping the topic. Reason: {e}",
                "Failed writing to ulog",
                style="yellow",
            )

        ui.advance_all(t_name)


def ulog_sequence_extractor():
    """Command-line entrypoint, with the same arguments as `mcap_sequence_extractor`."""
    parser = argparse.ArgumentParser(
        description="Extracts sequences from Mosaico and encodes them as ulog files"
    )
    _add_common_arguments(
        parser,
        output_flag="--ulog_path",
        output_help="Path where to save the ulog file",
    )
    args = parser.parse_args()
    ULogSequenceExtractor(ULogExtractorConfig(**_common_config_kwargs(args))).run()


if __name__ == "__main__":
    ulog_sequence_extractor()

"""Line truncation remains a bounded preview over unchanged raw byte positions."""

import unittest

from autoagent.agent.execution.preview import preview


class ExecutionPreviewTests(unittest.TestCase):
    def test_split_utf8_noise_line_keeps_neighboring_lines_and_one_marker(self):
        """A long line crossing collection chunks cannot remove surrounding lines or split UTF-8."""
        pieces = [b"head\n", *[("中" * 2000).encode() for _ in range(15)], b"\ntail\n"]
        offset, chunks = 0, []
        for piece in pieces:
            chunks.append((offset, "stdout", piece))
            offset += len(piece)
        result = preview(chunks, 0, offset, max_bytes=4096, line_bytes=2048, tail=True)
        self.assertTrue(result["stdout"].startswith("head\n"))
        self.assertTrue(result["stdout"].endswith("\ntail\n"))
        self.assertEqual(result["stdout"].count("..."), 1)
        self.assertNotIn("�", result["stdout"])
        self.assertEqual(result["next_cursor"], offset)
        self.assertGreater(result["omitted_line_bytes"], 80000)

    def test_small_pages_advance_by_raw_bytes_without_repeating_or_losing_content(self):
        """A partial line consumes only the raw prefix actually returned, including multibyte text."""
        raw = "a中b文c\n".encode()
        chunks, cursor, output = [(0, "stdout", raw)], 0, []
        while cursor < len(raw):
            result = preview(chunks, cursor, len(raw), max_bytes=4, line_bytes=64, tail=False)
            self.assertGreater(result["next_cursor"], cursor)
            output.append(result["stdout"])
            cursor = result["next_cursor"]
        self.assertEqual("".join(output), raw.decode())

    def test_interleaved_streams_track_line_caps_independently(self):
        """Noise on stdout cannot truncate a short stderr message or reset the stdout line budget."""
        first, warning, rest = b"x" * 8000, b"important warning\n", b"x" * 8000 + b"\n"
        chunks = [(0, "stdout", first), (len(first), "stderr", warning),
            (len(first) + len(warning), "stdout", rest)]
        end = len(first) + len(warning) + len(rest)
        result = preview(chunks, 0, end, max_bytes=4096, line_bytes=2048, tail=True)
        self.assertEqual(result["stderr"], warning.decode())
        self.assertEqual(result["stdout"].count("..."), 1)
        self.assertEqual(result["next_cursor"], end)

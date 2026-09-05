from __future__ import annotations

import unittest

from workflows.orchestration import ReviewReport, review_completed_event
from workflows.react_assistant import get_current_weather


class ProjectFunctionTests(unittest.TestCase):
    """Test isolated project logic that is not duplicated by an Eval Case."""

    def test_release_review_event_exposes_only_its_public_fields(self) -> None:
        report = ReviewReport(
            service="payments-api",
            decision="approved",
            review_rounds=2,
            path="specialist",
            notes=("internal detail",),
        )

        self.assertEqual(
            {
                "service": "payments-api",
                "decision": "approved",
                "review_rounds": 2,
                "path": "specialist",
            },
            review_completed_event(report),
        )

    def test_weather_tool_rejects_an_unsupported_city(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unsupported sample city"):
            get_current_weather("Atlantis")


if __name__ == "__main__":
    unittest.main()

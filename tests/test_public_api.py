from __future__ import annotations

import unittest

import autoagent


EXPECTED_ROOT_AUTHORING_API = {
    "ArtifactRef",
    "BackoffPolicy",
    "CapabilityRef",
    "CapabilitySelectionPolicy",
    "ConditionContext",
    "EdgePolicy",
    "FailurePolicy",
    "InputMapping",
    "InputMappingContext",
    "MapAggregationContext",
    "MapItemSelectionContext",
    "MapPolicy",
    "NodePolicy",
    "OutputBinding",
    "OutputBindingContext",
    "ReplicationAggregationContext",
    "ReplicationPolicy",
    "RecoveryPolicy",
    "ResourcePolicy",
    "RetryPolicy",
    "SystemCommand",
    "StreamReducer",
    "StreamingResult",
    "TimeoutPolicy",
    "UserEventMapping",
    "Workflow",
    "WorkflowPolicy",
    "workflow_hook",
    "streaming_result",
}


class RootPublicApiTests(unittest.TestCase):
    def test_root_all_is_the_stable_authoring_contract(self) -> None:
        self.assertEqual(set(autoagent.__all__), EXPECTED_ROOT_AUTHORING_API)
        self.assertEqual(len(autoagent.__all__), len(EXPECTED_ROOT_AUTHORING_API))

    def test_every_public_name_is_importable(self) -> None:
        for name in autoagent.__all__:
            with self.subTest(name=name):
                self.assertTrue(hasattr(autoagent, name))

    def test_runtime_and_hosting_types_are_not_in_authoring_contract(self) -> None:
        internal_names = {
            "AutoAgentApp",
            "AutoAgentServer",
            "AutoAgentSettings",
            "DatabaseBackend",
            "JsonRuntimeSerializer",
            "Operator",
            "OperatorContract",
            "PersistencePolicy",
            "RuntimeStore",
            "WorkflowDiagram",
        }

        self.assertTrue(internal_names.isdisjoint(autoagent.__all__))

    def test_nonrecommended_construction_types_are_not_in_authoring_contract(
        self,
    ) -> None:
        self.assertNotIn("Node", autoagent.__all__)
        self.assertNotIn("Edge", autoagent.__all__)
        self.assertNotIn("OperatorRef", autoagent.__all__)


if __name__ == "__main__":
    unittest.main()

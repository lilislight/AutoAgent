"""The UI example preserves inline namespaces and independent entry/exit wiring."""

import unittest

from autoagent import Workflow, Node, SubWorkflow
from autoagent.core.app import AutoAgentApp
from autoagent.core.compiler import WorkflowCompiler, WorkflowDefinitionSnapshot
from autoagent.core.runtime import RuntimeRepository
from examples.local_ui_demo.nested_demo import nested_review, Submission


class NestedWorkflowExampleTests(unittest.TestCase):
    def test_group_snapshot_keeps_direct_members_and_unambiguous_instance_paths(self):
        """Persist exact ownership for nested/reused definitions without interpreting dotted Node IDs."""
        ir = WorkflowCompiler().compile_or_raise(nested_review())
        self.assertEqual(len(ir.groups), 2)
        outer, inner = ir.groups
        self.assertEqual(outer.id, '["review"]')
        self.assertIsNone(outer.parent_group_id)
        self.assertEqual(outer.node_ids, ('review.standard_in', 'review.priority_in'))
        self.assertEqual(inner.id, '["review","checks"]')
        self.assertEqual(inner.parent_group_id, outer.id)
        snapshot = WorkflowDefinitionSnapshot.from_workflow_ir(ir).to_record()
        self.assertEqual(snapshot['definition']['groups'][1]['node_ids'], list(inner.node_ids))
        self.assertEqual(WorkflowDefinitionSnapshot.from_record(snapshot).definition_hash, ir.definition_hash)

    def test_nested_and_dotted_flat_definitions_have_distinct_revisions(self):
        """Equal flattened IDs do not erase the authoring boundary in a saved Revision."""
        def identity(value: Submission) -> Submission:
            return value
        compiler = WorkflowCompiler()
        grouped = compiler.compile_or_raise(Workflow('same', sub_workflows=[SubWorkflow('a', Workflow('child', nodes=[Node('value', identity)]))]))
        flat = compiler.compile_or_raise(Workflow('same', nodes=[Node('a.value', identity)]))
        self.assertNotEqual(grouped.definition_hash, flat.definition_hash)
        self.assertFalse(flat.groups)

    def test_nested_definition_keeps_two_entries_and_distinct_outward_exits(self):
        """Two inline levels expand into one graph with separate parent connections."""
        ir = WorkflowCompiler().compile_or_raise(nested_review())
        self.assertEqual(set(ir.entry_node_ids), {"submit_standard", "submit_priority"})
        self.assertEqual(set(ir.exit_node_ids), {"dispatch", "manual_review"})
        self.assertEqual(len(ir.nodes), 9)
        connections = {(edge.source, edge.target) for edge in ir.edges}
        self.assertIn(("submit_standard", "review.standard_in"), connections)
        self.assertIn(("submit_priority", "review.priority_in"), connections)
        self.assertIn(("review.checks.approved", "dispatch"), connections)
        self.assertIn(("review.checks.rejected", "manual_review"), connections)

    def test_both_entries_reach_both_exits_without_spawning_child_invocations(self):
        """All four paths run only the selected entry and matching output branch."""
        journal = RuntimeRepository()
        app = AutoAgentApp(runtime_repository=journal)
        self.addCleanup(app.close)
        definition = nested_review()
        for lane, amount, destination in (("standard", 200, "dispatch"),
                                          ("standard", 1800, "manual_review"),
                                          ("priority", 200, "dispatch"),
                                          ("priority", 800, "manual_review")):
            with self.subTest(lane=lane, amount=amount):
                result = app.invoke(definition, {"order_id": "test", "amount": amount},
                                    entry_node_id=f"submit_{lane}")
                self.assertEqual(result.status, "completed")
                invocation = journal.state(result.session_id).invocation
                completed = {occ.node_id for occ in invocation.scheduler.occurrences.values() if occ.status == "completed"}
                self.assertEqual(completed, {f"submit_{lane}", f"review.{lane}_in", "review.checks.assess",
                                             f"review.checks.{'approved' if destination == 'dispatch' else 'rejected'}", destination})
                self.assertFalse(invocation.child_plans)
                self.assertEqual(result.output[destination]["destination"], destination)
                self.assertEqual(result.output[destination]["lane"], lane)

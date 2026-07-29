from __future__ import annotations

import asyncio
from dataclasses import replace
import unittest
from uuid import UUID, uuid4

from autoagent.core.runtime import ExecutionSnapshot, RuntimeEvent, UserEvent
from autoagent.core.runtime.persistence import (
    BackendPersistenceError,
    PersistenceCoordinator,
    PersistenceEnvelope,
    PersistencePolicy,
    freeze_event_envelope,
    freeze_user_event_batch_envelope,
)


def _envelope(
    *,
    invocation_id: UUID | None = None,
    session_id: UUID | None = None,
    sequence: int = 1,
    estimated_bytes: int = 256,
) -> PersistenceEnvelope:
    invocation_id = invocation_id or uuid4()
    return PersistenceEnvelope(
        kind="event",
        namespace="default",
        session_id=session_id or uuid4(),
        session_updated_at_ms=10,
        invocation_id=invocation_id,
        invocation_state="running",
        execution_mode="normal",
        invocation_updated_at_ms=10,
        event=RuntimeEvent(
            invocation_id=invocation_id,
            sequence=sequence,
            event_type="state_change",
            event_name="invocation.running",
            subject_type="invocation",
            subject_id=str(invocation_id),
            occurred_at_ms=10,
        ),
        estimated_bytes=estimated_bytes,
        force_recovery_checkpoint=False,
    )


class PersistencePolicyTests(unittest.TestCase):
    def test_defaults_derive_low_and_hard_watermarks(self) -> None:
        policy = PersistencePolicy(queue_high_watermark_bytes=1_000)

        self.assertEqual(500, policy.queue_low_watermark_bytes)
        self.assertEqual(2_000, policy.queue_hard_watermark_bytes)

    def test_watermarks_must_be_strictly_ordered(self) -> None:
        with self.assertRaisesRegex(ValueError, "low < high < hard"):
            PersistencePolicy(
                queue_low_watermark_bytes=100,
                queue_high_watermark_bytes=100,
                queue_hard_watermark_bytes=200,
            )


class PersistenceEnvelopeTests(unittest.TestCase):
    def test_freeze_detaches_nested_event_payload_from_runtime_owner(self) -> None:
        invocation_id = uuid4()
        mutable = {"items": [{"value": "before"}]}
        event = RuntimeEvent(
            invocation_id=invocation_id,
            sequence=1,
            event_type="state_change",
            event_name="invocation.running",
            subject_type="invocation",
            subject_id=str(invocation_id),
            occurred_at_ms=10,
            payload={"mutable": mutable},
        )

        envelope = freeze_event_envelope(
            namespace="default",
            session_id=uuid4(),
            session_updated_at_ms=10,
            invocation_id=invocation_id,
            invocation_state="running",
            execution_mode="normal",
            invocation_updated_at_ms=10,
            invocation_result=None,
            invocation_error=None,
            event=event,
            recovery_snapshot=None,
            force_recovery_checkpoint=False,
        )
        mutable["items"][0]["value"] = "after"

        self.assertEqual(
            "before",
            envelope.event.payload["mutable"]["items"][0]["value"],
        )
        self.assertGreater(envelope.estimated_bytes, 256)


class PersistenceCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.coordinator = PersistenceCoordinator(
            PersistencePolicy(
                queue_low_watermark_bytes=250,
                queue_high_watermark_bytes=500,
                queue_hard_watermark_bytes=1_000,
                admission_timeout_ms=0,
            )
        )
        self.wake_count = 0
        self.coordinator.bind_consumer(self._wake)

    def _wake(self) -> None:
        self.wake_count += 1

    async def _publish(self, envelope: PersistenceEnvelope) -> None:
        reservation = self.coordinator.try_reserve(envelope)
        assert reservation is not None
        self.coordinator.publish(reservation, envelope)

    async def test_reservation_accounts_memory_before_consumer_prepares(self) -> None:
        envelope = _envelope(estimated_bytes=400)

        reservation = self.coordinator.try_reserve(envelope)

        assert reservation is not None
        self.assertEqual(1, self.coordinator.pending_count)
        self.assertEqual(400, self.coordinator.pending_bytes)
        self.assertEqual(0, self.wake_count)

        self.coordinator.publish(reservation, envelope)
        self.assertEqual(1, self.wake_count)
        self.assertEqual((envelope,), self.coordinator.take(1))
        self.assertEqual(400, self.coordinator.pending_bytes)

    def test_binding_the_same_consumer_is_idempotent(self) -> None:
        self.coordinator.bind_consumer(self._wake)

    async def test_incoming_events_are_fair_across_sessions(self) -> None:
        first_session = uuid4()
        second_session = uuid4()
        first_invocation = uuid4()
        first = _envelope(
            session_id=first_session,
            invocation_id=first_invocation,
            sequence=1,
        )
        second = _envelope(
            session_id=first_session,
            invocation_id=first_invocation,
            sequence=2,
        )
        other = _envelope(session_id=second_session)

        await self._publish(first)
        await self._publish(second)
        await self._publish(other)

        self.assertEqual(1, self.wake_count)
        self.assertEqual(
            (first, other, second),
            self.coordinator.take(3),
        )

    async def test_newer_recovery_state_replaces_older_queued_copy(self) -> None:
        coordinator = PersistenceCoordinator(
            PersistencePolicy(
                queue_low_watermark_bytes=10_000,
                queue_high_watermark_bytes=20_000,
                queue_hard_watermark_bytes=40_000,
            )
        )
        coordinator.bind_consumer(self._wake)
        invocation_id = uuid4()
        session_id = uuid4()
        snapshot = ExecutionSnapshot(
            invocation_id=invocation_id,
            through_sequence=1,
            state={"payload": "x" * 2_000},
        )
        first = replace(
            _envelope(
                invocation_id=invocation_id,
                session_id=session_id,
                sequence=1,
                estimated_bytes=3_000,
            ),
            recovery_snapshot=snapshot,
        )
        second = replace(
            _envelope(
                invocation_id=invocation_id,
                session_id=session_id,
                sequence=2,
                estimated_bytes=3_000,
            ),
            recovery_snapshot=snapshot.model_copy(
                update={"through_sequence": 2}
            ),
        )
        for envelope in (first, second):
            reservation = coordinator.try_reserve(envelope)
            assert reservation is not None
            coordinator.publish(reservation, envelope)

        queued = coordinator.take(2)
        self.assertIsNone(queued[0].recovery_snapshot)
        self.assertEqual(2, queued[1].recovery_snapshot.through_sequence)
        self.assertLess(coordinator.pending_bytes, 6_000)

    async def test_exact_serialized_size_replaces_estimate(self) -> None:
        envelope = _envelope(estimated_bytes=400)
        await self._publish(envelope)
        self.coordinator.take(1)

        self.coordinator.adjust_size(envelope.id, 725)

        self.assertEqual(725, self.coordinator.pending_bytes)
        self.coordinator.mark_durable(
            envelope.id,
            envelope.invocation_id,
            envelope.event.sequence,
        )
        self.assertEqual(0, self.coordinator.pending_bytes)
        self.assertEqual(0, self.coordinator.pending_count)

    async def test_hard_watermark_degrades_without_blocking_producer(
        self,
    ) -> None:
        first = _envelope(estimated_bytes=700)
        second = _envelope(estimated_bytes=400)
        first_reservation = self.coordinator.try_reserve(first)
        assert first_reservation is not None

        rejected = self.coordinator.try_reserve(second)

        self.assertIsNone(rejected)
        self.coordinator.cancel(first_reservation)
        second_reservation = self.coordinator.try_reserve(second)
        assert second_reservation is not None
        self.coordinator.cancel(second_reservation)

    async def test_invocation_failure_discards_only_that_journal(self) -> None:
        failed_id = uuid4()
        healthy_id = uuid4()
        failed = _envelope(invocation_id=failed_id, estimated_bytes=300)
        healthy = _envelope(invocation_id=healthy_id, estimated_bytes=300)
        await self._publish(failed)
        await self._publish(healthy)

        error = self.coordinator.fail_invocation(
            failed_id,
            failed.event.sequence,
            ValueError("not serializable"),
        )

        self.assertEqual("unserializable", self.coordinator.status(failed_id, 1))
        self.assertEqual("pending", self.coordinator.status(healthy_id, 1))
        self.assertEqual((healthy,), self.coordinator.take(10))
        self.assertEqual(300, self.coordinator.pending_bytes)
        self.assertEqual(failed_id, error.invocation_id)

    async def test_backend_unavailable_does_not_reject_below_watermark(
        self,
    ) -> None:
        self.coordinator.mark_unavailable(OSError("database unavailable"))

        await asyncio.wait_for(
            self.coordinator.await_admission(),
            timeout=0.05,
        )
        self.assertEqual("unavailable", self.coordinator.health.state)

    async def test_consumer_wake_failure_marks_backend_unavailable(self) -> None:
        coordinator = PersistenceCoordinator(PersistencePolicy())

        def fail_to_wake() -> None:
            raise RuntimeError("consumer loop stopped")

        coordinator.bind_consumer(fail_to_wake)
        envelope = _envelope()
        reservation = coordinator.try_reserve(envelope)
        assert reservation is not None

        coordinator.publish(reservation, envelope)

        self.assertEqual(
            "degraded",
            coordinator.status(envelope.invocation_id, 1),
        )
        self.assertEqual(1, coordinator.pending_count)
        await coordinator.await_admission()

    async def test_durable_cursor_advances_after_sink_acknowledgement(self) -> None:
        envelope = _envelope()
        await self._publish(envelope)
        self.assertEqual(
            0,
            self.coordinator.durable_sequence(envelope.invocation_id),
        )

        self.coordinator.mark_durable(
            envelope.id,
            envelope.invocation_id,
            envelope.event.sequence,
        )
        self.assertEqual(
            envelope.event.sequence,
            self.coordinator.durable_sequence(envelope.invocation_id),
        )

    async def test_runtime_failure_keeps_independent_user_event_batch(self) -> None:
        invocation_id = uuid4()
        session_id = uuid4()
        runtime = _envelope(
            invocation_id=invocation_id,
            session_id=session_id,
        )
        user_batch = freeze_user_event_batch_envelope(
            namespace="default",
            session_id=session_id,
            invocation_id=invocation_id,
            events=(
                UserEvent(
                    invocation_id=invocation_id,
                    sequence=1,
                    type="agent_output",
                    data={"output": "done"},
                    node_id="finish",
                    node_execution_id=uuid4(),
                    occurred_at_ms=10,
                ),
            ),
        )
        user_batch = replace(user_batch, estimated_bytes=256)
        await self._publish(runtime)
        await self._publish(user_batch)

        self.coordinator.fail_invocation(
            invocation_id,
            1,
            RuntimeError("runtime serialization failed"),
        )

        self.assertEqual(
            ("user_event_batch",),
            tuple(envelope.kind for envelope in self.coordinator.take(10)),
        )

    def test_user_event_gap_does_not_degrade_runtime_journal(self) -> None:
        invocation_id = uuid4()
        self.coordinator.remember_admission_durable(invocation_id)
        self.coordinator.remember_durable(invocation_id, 3)

        self.coordinator.degrade_user_events(invocation_id, 2, "user gap")

        self.assertEqual("durable", self.coordinator.status(invocation_id, 3))
        self.assertIsNotNone(
            self.coordinator.user_event_gap(invocation_id)
        )

    async def test_pending_user_events_do_not_hold_runtime_status_open(
        self,
    ) -> None:
        invocation_id = uuid4()
        session_id = uuid4()
        self.coordinator.remember_admission_durable(invocation_id)
        self.coordinator.remember_durable(invocation_id, 3)
        batch = freeze_user_event_batch_envelope(
            namespace="default",
            session_id=session_id,
            invocation_id=invocation_id,
            events=(
                UserEvent(
                    invocation_id=invocation_id,
                    sequence=4,
                    type="agent_output",
                    data={"output": "done"},
                    node_id="finish",
                    node_execution_id=uuid4(),
                    occurred_at_ms=10,
                ),
            ),
        )
        batch = replace(batch, estimated_bytes=256)
        await self._publish(batch)

        self.assertEqual("durable", self.coordinator.status(invocation_id, 3))
        self.assertEqual(
            "pending",
            self.coordinator.user_event_status(invocation_id, 4),
        )

        self.coordinator.mark_user_events_durable(
            batch.id,
            invocation_id,
            4,
        )
        self.assertEqual(
            "durable",
            self.coordinator.user_event_status(invocation_id, 4),
        )

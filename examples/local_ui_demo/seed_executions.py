"""Create real executions, including a Session shared by multiple Workflows.

Run with the Server stopped, using the same AUTOAGENT_DATABASE_URL as the Server:
    .venv/bin/python -m examples.local_ui_demo.seed_executions
"""

import asyncio
import argparse
from pathlib import Path

from autoagent.host import AutoAgentHost


async def seed_mapped_review(host, *, session_id=None, children_count=5):
    """Persist real Map Calls and wait until every spawned Worker completes."""
    documents = ["Architecture review", "API review", "Runtime review", "Persistence review", "UI review"]
    documents = (documents + [f"Review task {index + 1:02d}" for index in range(5, children_count)])[:children_count]
    ref = await host.execute("document-batch-review", {"documents": documents}, session_id=session_id, entry_node_id="review_documents")
    result = await host.join(ref)
    if result.status != "completed":
        raise RuntimeError(f"Mapped review failed: {result.status}")
    for _ in range(100):
        children = (await host.list_child_executions(ref.invocation_id, limit=children_count)).items
        if len(children) == children_count and all(child["status"] == "completed" for child in children):
            break
        await asyncio.sleep(.05)
    else:
        raise RuntimeError(f"The {children_count} spawned review Workers did not complete")
    print("Map + Map Spawn:", result.status, "Invocation:", ref.invocation_id,
          "Session:", ref.session_id, "Completed children:", len(children))
    return ref


async def seed_replay_review(host, rounds):
    """Create a long real Loop history for testing bounded Replay windows."""
    ref = await host.execute("document-review-loop", {"text": "Replay performance review", "rounds": rounds}, entry_node_id="prepare")
    result = await host.join(ref)
    if result.status != "completed":
        raise RuntimeError(f"Replay review failed: {result.status}")
    print("Replay Loop:", rounds, "passes", result.status, "Invocation:", ref.invocation_id, "Session:", ref.session_id)
    return ref


async def seed_parallel_waits(host):
    """Create twenty persisted pending responses without resolving them."""
    ref = await host.execute("parallel-approvals", {"subject": "Release readiness review"}, entry_node_id="prepare")
    result = await host.join(ref)
    if result.status != "waiting" or len(result.waits) != 20:
        raise RuntimeError(f"Expected twenty parallel Waits; got {result.status}, {len(result.waits)} Waits")
    print("Parallel approvals:", result.status, "Pending Waits:", len(result.waits),
          "Invocation:", ref.invocation_id, "Session:", ref.session_id)
    return ref


async def seed_revisions():
    """Create genuine execution history across two Revisions and two Workflows."""
    from .revision_demo import build_revision_review
    configured = AutoAgentHost.from_project(Path(__file__).parent)
    async with AutoAgentHost(workflows=[build_revision_review("1")], settings=configured.settings,
        root=configured.root, environment=configured.environment) as host:
        ref = await host.execute("revision-review-demo", {"text": "Review using the first Revision"}, entry_node_id="prepare")
        await host.join(ref)
        session = ref.session_id
        await host.create_checkpoint(session, pinned=True)
    async with AutoAgentHost.from_project(Path(__file__).parent) as host:
        for text in ("Review using the current Revision", "Review the follow-up"):
            ref = await host.execute("revision-review-demo", {"text": text}, session_id=session, entry_node_id="prepare")
            await host.join(ref)
        await host.create_checkpoint(session, pinned=True)
        ref = await host.execute("document-formatting", {"text": "Format the review summary"}, session_id=session, entry_node_id="format")
        await host.join(ref)
    print("Revision comparison Workflow: revision-review-demo (v1 and v2)")
    print("Shared Session (2 Workflows, 3 Revisions, 4 Invocations):", session)


async def main(*, pagination=False, maps_only=False, children_count=5, replay_rounds=None, parallel_waits=False, revisions_only=False):
    if revisions_only:
        await seed_revisions()
        return
    async with AutoAgentHost.from_project(Path(__file__).parent) as host:
        if parallel_waits:
            await seed_parallel_waits(host)
            return
        if replay_rounds is not None:
            await seed_replay_review(host, replay_rounds)
            return
        if maps_only:
            await seed_mapped_review(host, children_count=children_count)
            return
        shared_session = None
        for name, approved, amount in (("approved", True, 600), ("rejected", False, 200), ("manual-review", True, 2400), ("risk-service-failure", True, 100), ("failed", True, -1), ("waiting", None, 450)):
            ref = await host.execute("order-processing-demo", {"order_id":name, "amount":amount, "stock":3}, entry_node_id="prepare.normalize")
            result = await host.join(ref)
            if approved is not None:
                result = await host.respond(ref, result.waits[0].id, {"approved":approved})
                result = await host.join(ref)
            print(name, result.status, ref.invocation_id)
            if name == "approved":
                shared_session = ref.session_id
        ref = await host.execute("document-formatting", {"text":"example output"}, entry_node_id="format")
        result = await host.join(ref)
        print("document", result.status, ref.invocation_id)
        assert shared_session is not None
        # Reuse the Order's root Session for two independent Document roots.
        # The Order's audit Child has its own Session and is linked by parent
        # and root Invocation IDs, independently of this shared Session.
        for text in ("shared-session receipt", "shared-session summary"):
            ref = await host.execute("document-formatting", {"text":text}, session_id=shared_session, entry_node_id="format")
            result = await host.join(ref)
            print(text, result.status, ref.invocation_id)
        for rounds in (3, 24):
            ref = await host.execute("document-review-loop", {"text":"Review this document", "rounds":rounds},
                                     session_id=shared_session if rounds == 3 else None, entry_node_id="prepare")
            result = await host.join(ref)
            print(f"Loop ({rounds} passes)", result.status, ref.invocation_id, "Session:", ref.session_id)
        await seed_mapped_review(host, session_id=shared_session, children_count=children_count)
        print("Shared Session (4 Workflows, 5 Invocations):", shared_session)
        if pagination:
            for index in range(60):
                ref = await host.execute("document-formatting", {"text":f"Pagination Session {index + 1:02d}"}, entry_node_id="format")
                await host.join(ref)
            print("Created 60 additional Sessions for pagination.")
            paged_session = None
            for index in range(60):
                ref = await host.execute("document-formatting", {"text":f"Pagination Invocation {index + 1:02d}"}, session_id=paged_session, entry_node_id="format")
                paged_session = ref.session_id
                await host.join(ref)
            print("Pagination Session (60 Invocations):", paged_session)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pagination', action='store_true', help='Also create 60 Sessions and one Session containing 60 Invocations.')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--replay-rounds', type=int, help='Only create a Loop replay example with 1–200 passes (120 yields over 500 Events).')
    mode.add_argument('--maps-only', action='store_true', help='Only execute the Map and Map Spawn example.')
    mode.add_argument('--revisions-only', action='store_true', help='Only create Revision Compare and cross-Revision Session examples.')
    mode.add_argument('--parallel-waits', action='store_true', help='Only create one Invocation with twenty pending external approvals.')
    parser.add_argument('--children', type=int, default=5, help='Number of review Child Tasks (1–200, default: 5).')
    args = parser.parse_args()
    if args.replay_rounds is not None and not 1 <= args.replay_rounds <= 200:
        parser.error('--replay-rounds must be between 1 and 200')
    if not 1 <= args.children <= 200:
        parser.error('--children must be between 1 and 200')
    asyncio.run(main(pagination=args.pagination, maps_only=args.maps_only, children_count=args.children, replay_rounds=args.replay_rounds, parallel_waits=args.parallel_waits, revisions_only=args.revisions_only))

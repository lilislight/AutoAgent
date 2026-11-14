"""Live product settings and resource updates reuse SDK native Wait resolution."""

from autoagent.agent.runtime.approvals import AgentApprovals as SDKAgentApprovals


class AgentApprovals(SDKAgentApprovals):
    async def changed(self, session_id=None):
        """Reevaluate only this product's execution families after a live change."""
        await self.waiting(session_id)

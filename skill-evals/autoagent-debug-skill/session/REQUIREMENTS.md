# Investigate an expense Session

Session ID `INCIDENT_ID`

A user set their automatic approval limit to 12,500 cents. The settings
receipt confirmed that limit. A later expense for 12,400 cents in the same
Session was sent for review, although amounts at or below the limit should
be allowed automatically.

Find the relevant Invocations in this Session, establish where the value
first diverged, and make a focused repair. Verify both the saved limit and
the later classification with the supplied business Eval.

Follow `autoagent-debug-skill/SKILL.md` in this workspace and preserve the
original Session.

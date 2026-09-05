import { CircleDashed, MessageSquareText } from "lucide-react";
import { memo } from "react";
import type { UserEvent } from "../types";

interface Props {
  events: readonly UserEvent[];
  hiddenCount: number;
  live: boolean;
}

export const UserEventTimeline = memo(function UserEventTimeline({
  events,
  hiddenCount,
  live,
}: Props) {
  if (events.length === 0) {
    return (
      <div className="user-events-empty">
        <CircleDashed size={16} />
        <span>No User Events yet.</span>
      </div>
    );
  }
  return (
    <div className="user-events" role="list" aria-label="User Events">
      {hiddenCount > 0 && (
        <div className="timeline-truncated">
          User Event cursor starts after {hiddenCount.toLocaleString()} earlier positions
        </div>
      )}
      {events.map((event) => (
        <div className="user-event-row" key={event.id} role="listitem">
          <span className="user-event-icon"><MessageSquareText size={13} /></span>
          <span className="user-event-copy">
            <strong>{event.kind}</strong>
            <code>{payloadText(event.payload)}</code>
          </span>
          {event.occurrence_id && (
            <span className="user-event-occurrence">{event.occurrence_id}</span>
          )}
          <span className="user-event-sequence">#{event.sequence}</span>
        </div>
      ))}
      <span className={`user-event-channel ${live ? "active" : ""}`}>
        {live ? "Live UserEvent stream" : "UserEvent history"}
      </span>
    </div>
  );
});

function payloadText(value: unknown): string {
  try {
    const encoded = JSON.stringify(value);
    return encoded ?? String(value);
  } catch {
    return "[unrenderable data]";
  }
}

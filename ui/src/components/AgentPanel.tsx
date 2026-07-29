import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Bot,
  Brain,
  ChevronRight,
  CircleAlert,
  LoaderCircle,
  Wrench,
  X,
} from "lucide-react";
import { AnimatePresence, motion } from "motion/react";

import {
  getAllUserEvents,
  getInvocation,
  listAgentInvocationNeighbors,
  subscribeToUserEvents,
} from "../api";
import {
  logicalUserEventKeys,
  projectAgentInvocation,
  selectAgentInvocationAnchor,
  shouldCountHydratedEventsAsUnread,
  type AgentInvocationView,
} from "../agentConversation";
import type { InvocationSummary, UserEvent } from "../types";

interface SessionCache {
  events: Map<string, UserEvent[]>;
  inputs: Map<string, unknown>;
  loaded: Set<string>;
  loading: Set<string>;
  unread: Set<string>;
  initializedAtMs: number;
}

interface AgentPanelProps {
  open: boolean;
  sessionId: string | null;
  invocationId: string | null;
  invocations: InvocationSummary[];
  onClose: () => void;
  onUnreadCountChange: (count: number) => void;
}

const TERMINAL_STATES = new Set([
  "completed",
  "failed",
  "cancelled",
  "interrupted",
]);

export function AgentPanel({
  open,
  sessionId,
  invocationId,
  invocations,
  onClose,
  onUnreadCountChange,
}: AgentPanelProps) {
  const caches = useRef(new Map<string, SessionCache>());
  const activeSessionId = useRef<string | null>(sessionId);
  activeSessionId.current = sessionId;
  const [revision, setRevision] = useState(0);
  const [streamConnected, setStreamConnected] = useState<boolean | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const preservingScroll = useRef<{ height: number; top: number } | null>(null);
  const appendScroll = useRef(false);
  const positioning = useRef(false);
  const focusGeneration = useRef(0);
  const knownInvocations = useRef(new Map<string, InvocationSummary>());
  const olderNeighbor = useRef(new Map<string, string | null>());
  const newerNeighbor = useRef(new Map<string, string | null>());
  const [visibleInvocationIds, setVisibleInvocationIds] = useState<string[]>([]);
  const [loadingDirection, setLoadingDirection] = useState<
    "anchor" | "older" | "newer" | null
  >(null);
  const [canLoadOlder, setCanLoadOlder] = useState(false);
  const [canLoadNewer, setCanLoadNewer] = useState(false);

  const orderedInvocations = useMemo(
    () => [...invocations].sort(
      (left, right) =>
        left.created_at_ms - right.created_at_ms ||
        left.id.localeCompare(right.id),
    ),
    [invocations],
  );
  const cache = sessionId ? getCache(caches.current, sessionId) : null;
  const latestInvocation = orderedInvocations.at(-1) ?? null;

  useEffect(() => {
    for (const invocation of orderedInvocations) {
      knownInvocations.current.set(invocation.id, invocation);
    }
    for (let index = 1; index < orderedInvocations.length; index += 1) {
      const older = orderedInvocations[index - 1];
      const newer = orderedInvocations[index];
      newerNeighbor.current.set(older.id, newer.id);
      olderNeighbor.current.set(newer.id, older.id);
    }
  }, [orderedInvocations]);

  const publishUnread = useCallback((target: SessionCache | null) => {
    const current = activeSessionId.current;
    if (
      target !== null &&
      (current === null || caches.current.get(current) !== target)
    ) return;
    onUnreadCountChange(target?.unread.size ?? 0);
  }, [onUnreadCountChange]);

  const mergeEvents = useCallback((
    target: SessionCache,
    invocationId: string,
    incoming: UserEvent[],
    countUnread: boolean,
  ) => {
    const current = target.events.get(invocationId) ?? [];
    const ids = new Set(current.map((event) => event.id));
    const additions = incoming.filter((event) => !ids.has(event.id));
    if (additions.length === 0) return false;
    target.events.set(
      invocationId,
      [...current, ...additions].sort(
        (left, right) => left.sequence - right.sequence,
      ),
    );
    if (countUnread) {
      for (const event of additions) {
        for (const key of logicalUserEventKeys(event)) {
          target.unread.add(`${invocationId}:${key}`);
        }
      }
    }
    return true;
  }, []);

  const loadInvocation = useCallback(async (
    target: SessionCache,
    invocation: InvocationSummary,
    countUnread: boolean,
  ) => {
    if (
      target.loaded.has(invocation.id) ||
      target.loading.has(invocation.id)
    ) return;
    target.loading.add(invocation.id);
    setRevision((value) => value + 1);
    try {
      const [loaded, detail] = await Promise.all([
        getAllUserEvents(invocation.id),
        getInvocation(invocation.id),
      ]);
      mergeEvents(target, invocation.id, loaded, countUnread && !open);
      target.inputs.set(invocation.id, detail.input);
      target.loaded.add(invocation.id);
    } finally {
      target.loading.delete(invocation.id);
      publishUnread(target);
      setRevision((value) => value + 1);
    }
  }, [mergeEvents, open, publishUnread]);

  useEffect(() => {
    publishUnread(cache);
    if (!cache || !latestInvocation) return;
    void loadInvocation(
      cache,
      latestInvocation,
      shouldCountHydratedEventsAsUnread(
        latestInvocation.created_at_ms,
        cache.initializedAtMs,
      ),
    );
  }, [
    cache,
    latestInvocation?.id,
    loadInvocation,
    publishUnread,
    sessionId,
  ]);

  useEffect(() => {
    if (!open || !cache) return;
    cache.unread.clear();
    publishUnread(cache);
  }, [cache, open, publishUnread, revision]);

  const latestLoaded = Boolean(
    cache && latestInvocation && cache.loaded.has(latestInvocation.id),
  );
  const latestSequence = cache && latestInvocation
    ? cache.events.get(latestInvocation.id)?.at(-1)?.sequence ?? 0
    : 0;

  useEffect(() => {
    if (
      !cache ||
      !latestInvocation ||
      !latestLoaded ||
      TERMINAL_STATES.has(latestInvocation.state)
    ) {
      setStreamConnected(null);
      return;
    }
    return subscribeToUserEvents(
      latestInvocation.id,
      latestSequence,
      (event) => {
        if (
          mergeEvents(
            cache,
            latestInvocation.id,
            [event],
            !open,
          )
        ) {
          publishUnread(cache);
          setRevision((value) => value + 1);
        }
      },
      setStreamConnected,
    );
  }, [
    cache,
    latestInvocation?.id,
    latestInvocation?.state,
    latestLoaded,
    mergeEvents,
    open,
    publishUnread,
  ]);

  useEffect(() => {
    if (!open || !cache || !sessionId) return;
    const generation = ++focusGeneration.current;
    let cancelled = false;
    const focus = async () => {
      setLoadingDirection("anchor");
      let target = invocationId
        ? knownInvocations.current.get(invocationId)
        : undefined;
      if (!target && invocationId) {
        const detail = await getInvocation(invocationId);
        if (detail.session_id === sessionId) {
          target = detail;
          knownInvocations.current.set(detail.id, detail);
        }
      }
      target ??= selectAgentInvocationAnchor(
        orderedInvocations,
        invocationId,
      ) ?? undefined;
      if (
        cancelled ||
        generation !== focusGeneration.current ||
        !target
      ) {
        setLoadingDirection(null);
        return;
      }
      setVisibleInvocationIds([target.id]);
      setCanLoadOlder(true);
      setCanLoadNewer(true);
      await loadInvocation(cache, target, false);
      if (cancelled || generation !== focusGeneration.current) return;
      positioning.current = true;
      setLoadingDirection(null);
      setRevision((value) => value + 1);
      requestAnimationFrame(() => {
        const element = scrollRef.current;
        const anchor = element?.querySelector<HTMLElement>(
          `[data-invocation-id="${target.id}"]`,
        );
        anchor?.scrollIntoView({ block: "center" });
        requestAnimationFrame(() => {
          positioning.current = false;
        });
      });
    };
    void focus().catch(() => {
      if (!cancelled && generation === focusGeneration.current) {
        setLoadingDirection(null);
      }
    });
    return () => {
      cancelled = true;
    };
  }, [
    cache,
    invocationId,
    latestInvocation?.id,
    loadInvocation,
    open,
    sessionId,
  ]);

  const resolveNeighbor = useCallback(async (
    anchorId: string,
    direction: "older" | "newer",
  ): Promise<string | null> => {
    const links = direction === "older"
      ? olderNeighbor.current
      : newerNeighbor.current;
    if (links.has(anchorId)) return links.get(anchorId) ?? null;
    if (!sessionId) return null;
    const page = await listAgentInvocationNeighbors(
      sessionId,
      anchorId,
      direction,
    );
    for (const invocation of page.items) {
      knownInvocations.current.set(invocation.id, invocation);
    }
    const chain = direction === "older"
      ? [...page.items, knownInvocations.current.get(anchorId)!]
      : [knownInvocations.current.get(anchorId)!, ...page.items];
    for (let index = 1; index < chain.length; index += 1) {
      const older = chain[index - 1];
      const newer = chain[index];
      newerNeighbor.current.set(older.id, newer.id);
      olderNeighbor.current.set(newer.id, older.id);
    }
    const remoteBoundary = direction === "older"
      ? chain[0]
      : chain.at(-1);
    if (!page.has_more && remoteBoundary) {
      links.set(remoteBoundary.id, null);
    }
    if (page.items.length === 0) links.set(anchorId, null);
    return links.get(anchorId) ?? null;
  }, [sessionId]);

  const loadNeighbor = useCallback(async (
    direction: "older" | "newer",
  ) => {
    if (
      !cache ||
      loadingDirection !== null ||
      visibleInvocationIds.length === 0
    ) return;
    const orderedVisible = [...visibleInvocationIds].sort((left, right) =>
      compareInvocations(
        knownInvocations.current.get(left)!,
        knownInvocations.current.get(right)!,
      )
    );
    const boundaryId = direction === "older"
      ? orderedVisible[0]
      : orderedVisible.at(-1)!;
    setLoadingDirection(direction);
    try {
      const candidateId = await resolveNeighbor(boundaryId, direction);
      if (!candidateId) {
        if (direction === "older") setCanLoadOlder(false);
        else setCanLoadNewer(false);
        return;
      }
      const candidate = knownInvocations.current.get(candidateId);
      if (!candidate) return;
      const element = scrollRef.current;
      if (direction === "older" && element) {
        preservingScroll.current = {
          height: element.scrollHeight,
          top: element.scrollTop,
        };
      } else if (direction === "newer") {
        appendScroll.current = true;
      }
      await loadInvocation(cache, candidate, false);
      setVisibleInvocationIds((current) => (
        current.includes(candidate.id)
          ? current
          : [...current, candidate.id]
      ));
    } finally {
      setLoadingDirection(null);
      setRevision((value) => value + 1);
    }
  }, [
    cache,
    loadInvocation,
    loadingDirection,
    resolveNeighbor,
    visibleInvocationIds,
  ]);

  const loadedViews = useMemo(() => {
    if (!cache) return [];
    return visibleInvocationIds
      .map((id) => knownInvocations.current.get(id))
      .filter((value): value is InvocationSummary => Boolean(value))
      .sort(compareInvocations)
      .filter((invocation) => cache.loaded.has(invocation.id))
      .map((invocation) =>
        projectAgentInvocation(
          invocation,
          cache.events.get(invocation.id) ?? [],
        )
      );
  }, [cache, revision, visibleInvocationIds]);

  useEffect(() => {
    const preserved = preservingScroll.current;
    const element = scrollRef.current;
    if (!preserved || !element) return;
    element.scrollTop =
      preserved.top + element.scrollHeight - preserved.height;
    preservingScroll.current = null;
    return;
  }, [revision, visibleInvocationIds]);

  useEffect(() => {
    const element = scrollRef.current;
    if (!appendScroll.current || !element) return;
    element.scrollTop = element.scrollHeight;
    appendScroll.current = false;
  }, [revision, visibleInvocationIds]);

  const loading = loadingDirection !== null;

  return (
    <AnimatePresence>
      {open && (
        <motion.div
          className="agent-overlay"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) onClose();
          }}
        >
          <motion.section
            className="agent-panel"
            initial={{ opacity: 0, y: 14, scale: 0.985 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 10, scale: 0.99 }}
          >
            <header className="agent-panel-header">
              <div>
                <Bot size={18} />
                <span>Agent Activity</span>
                {streamConnected === true && <i>Live</i>}
              </div>
              <button
                type="button"
                className="icon-button"
                onClick={onClose}
                aria-label="Close Agent Activity"
              >
                <X size={17} />
              </button>
            </header>
            <div
              className="agent-conversation"
              ref={scrollRef}
              onScroll={(event) => {
                if (positioning.current || loading) return;
                const element = event.currentTarget;
                if (element.scrollTop < 48 && canLoadOlder) {
                  void loadNeighbor("older");
                  return;
                }
                if (
                  element.scrollHeight - element.scrollTop
                    - element.clientHeight < 48 &&
                  canLoadNewer
                ) {
                  void loadNeighbor("newer");
                }
              }}
            >
              {(loadingDirection === "anchor" ||
                loadingDirection === "older") && (
                <div className="agent-history-loader" aria-live="polite">
                  <LoaderCircle className="spin" size={15} />
                  <span>
                    {loadingDirection === "anchor"
                      ? "Locating Current Invocation"
                      : "Loading Earlier Activity"}
                  </span>
                  <i />
                  <i />
                  <i />
                </div>
              )}
              {loadedViews.length === 0 && (
                <div className="agent-empty">
                  {loading
                    ? "Reconstructing the latest Invocation…"
                    : "This Session has no UserEvents."}
                </div>
              )}
              {loadedViews.map((view) => (
                <AgentInvocation
                  key={view.invocation.id}
                  value={view}
                  input={cache?.inputs.get(view.invocation.id)}
                />
              ))}
              {loadingDirection === "newer" && (
                <div className="agent-history-loader" aria-live="polite">
                  <LoaderCircle className="spin" size={15} />
                  <span>Loading Newer Activity</span>
                  <i />
                  <i />
                  <i />
                </div>
              )}
            </div>
          </motion.section>
        </motion.div>
      )}
    </AnimatePresence>
  );
}

function AgentInvocation({
  value,
  input,
}: {
  value: AgentInvocationView;
  input: unknown;
}) {
  const collapseDetails = value.agentOutputSequence !== null;
  return (
    <article
      className="agent-invocation"
      data-invocation-id={value.invocation.id}
    >
      <header>
        <span>Invocation {shortId(value.invocation.id)}</span>
        <time>{formatTime(value.invocation.created_at_ms)}</time>
        <i className={`state-${value.invocation.state}`}>{value.invocation.state}</i>
      </header>
      {input !== undefined && (
        <div className="agent-user-message">
          {renderChatValue(primaryInvocationInput(input))}
        </div>
      )}
      {value.activity.length > 0 && (
        <details className="agent-trace-details" open={!collapseDetails}>
          <summary>
            <ChevronRight className="agent-details-chevron" size={14} />
            <span className="agent-details-closed">Show Reasoning and Tools</span>
            <span className="agent-details-open">Hide Reasoning and Tools</span>
          </summary>
          <div className="agent-activity-list">
            {value.activity.map((item) => (
              <AgentActivityItemView key={item.key} item={item} />
            ))}
          </div>
        </details>
      )}
      {value.agentOutputSequence !== null && (
        <div className="agent-output">
          {renderChatValue(value.agentOutput)}
        </div>
      )}
    </article>
  );
}

function AgentActivityItemView({
  item,
}: {
  item: AgentInvocationView["activity"][number];
}) {
  if (item.kind === "thinking") {
    const message = item.message;
    return (
      <details className="agent-thinking" open={!message.completed}>
        <summary>
          <Brain size={14} />
          <span>Thinking</span>
          <time>{durationLabel(
            message.reasoningStartedAtMs,
            message.reasoningEndedAtMs,
          )}</time>
        </summary>
        <p>{message.reasoning}</p>
      </details>
    );
  }
  if (item.kind === "message") {
    return (
      <div className="agent-assistant-message">
        {item.message.content}
        {!item.message.completed && <span className="agent-caret" />}
      </div>
    );
  }
  if (item.kind === "tool") {
    const tool = item.tool;
    return (
      <details className={`agent-tool ${tool.error ? "has-error" : ""}`}>
        <summary>
          <Wrench size={14} />
          <span>{tool.name ?? tool.toolCallId ?? "Tool Call"}</span>
          <time>{durationLabel(tool.requestedAtMs, tool.completedAtMs)}</time>
        </summary>
        {tool.rawArguments && (
          <JsonValue label="Arguments" value={parseJson(tool.rawArguments)} />
        )}
        {tool.error ? (
          <JsonValue label="Error" value={tool.error} error />
        ) : tool.completedAtMs !== null ? (
          <JsonValue label="Result" value={tool.result} />
        ) : (
          <p className="agent-tool-pending">Waiting for Tool Result…</p>
        )}
      </details>
    );
  }
  const { event, tone } = item.value;
  return (
    <details className={`agent-generic-card tone-${tone}`}>
      <summary>
        {tone === "error" ? <CircleAlert size={14} /> : <Bot size={14} />}
        {humanizeEventType(event.type)}
        <time>{formatTime(event.occurred_at_ms)}</time>
      </summary>
      <JsonValue value={event.data} />
    </details>
  );
}

function JsonValue({
  label,
  value,
  error = false,
}: {
  label?: string;
  value: unknown;
  error?: boolean;
}) {
  return (
    <div className={`agent-json ${error ? "has-error" : ""}`}>
      {label && <span>{label}</span>}
      <pre>{JSON.stringify(value, null, 2)}</pre>
    </div>
  );
}

function renderChatValue(value: unknown) {
  if (typeof value === "string") return <p>{value}</p>;
  return <pre>{JSON.stringify(value, null, 2)}</pre>;
}

function getCache(
  caches: Map<string, SessionCache>,
  sessionId: string,
): SessionCache {
  let cache = caches.get(sessionId);
  if (!cache) {
    cache = {
      events: new Map(),
      inputs: new Map(),
      loaded: new Set(),
      loading: new Set(),
      unread: new Set(),
      initializedAtMs: Date.now(),
    };
    caches.set(sessionId, cache);
  }
  return cache;
}

function compareInvocations(
  left: InvocationSummary,
  right: InvocationSummary,
): number {
  return (
    left.created_at_ms - right.created_at_ms ||
    left.id.localeCompare(right.id)
  );
}

function primaryInvocationInput(value: unknown): unknown {
  if (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    "input" in value
  ) {
    return (value as Record<string, unknown>).input;
  }
  return value;
}

function humanizeEventType(value: string): string {
  return value
    .split("_")
    .filter(Boolean)
    .map((part) => part[0]?.toUpperCase() + part.slice(1))
    .join(" ");
}

function durationLabel(start: number | null, end: number | null): string {
  if (start === null || end === null) return "";
  const elapsed = Math.max(0, end - start);
  if (elapsed < 1_000) return `${elapsed}ms`;
  return `${(elapsed / 1_000).toFixed(elapsed < 10_000 ? 1 : 0)}s`;
}

function formatTime(value: number): string {
  return new Date(value).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function shortId(value: string): string {
  return value.slice(0, 8);
}

function parseJson(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

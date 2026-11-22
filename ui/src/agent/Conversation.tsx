import { memo, useLayoutEffect, useMemo, useRef, useState, type ReactNode, type CSSProperties } from 'react';
import { conversationTools, type ConversationState } from './conversationState.js';
import { conversationEntries } from './conversationTurns.js';
import type { UsageLoader } from './UsageButton.js';
import { ConversationMessage, ConversationTurn } from './ConversationTurn.js';
import { executionStatus } from './executionState.js';
import type { AgentExecution, AgentInvocation, AgentTurnIndexItem } from './types.js';
import { AgentIcon } from './AgentIcon.js';
import { BusyLabel, ConversationSkeleton } from '../components/LoadingState.js';
import { Tooltip } from '../components/Tooltip.js';
import { ScrollFollowing } from './scrollFollowing.js';
import { ConversationNavigator } from './ConversationNavigator.js';
import { conversationLandmarks, mergeLandmarks, type ConversationLandmark } from './conversationLandmarks.js';
export { formatTokens } from './ConversationTurn.js';

const noInvocations: AgentInvocation[] = [];
const noIndex: AgentTurnIndexItem[] = [];
export const Conversation = memo(function Conversation({ state, loading, error, onRetry, invocations = noInvocations, execution,
  active = true, session = '', loadUsage, onFork, header, before, after, footer, running = false, mobile = false,
  turnIndex = noIndex, archived = false, onLoadTurn, onLatest, onLoadOlder, loadingOlder = false, switchingWindow = false, onCancelJump }: {
  session?: string; loadUsage?: UsageLoader; onFork?: (invocation: string, destination: string, signal: AbortSignal) => Promise<void>;
  state: ConversationState; loading: boolean; error?: Error | null; onRetry: () => void;
  invocations?: AgentInvocation[]; execution?: AgentExecution; active?: boolean;
  turnIndex?: AgentTurnIndexItem[]; archived?: boolean; onLoadTurn?: (invocation: string) => void; onLatest?: () => void;
  onLoadOlder?: () => void; loadingOlder?: boolean; switchingWindow?: boolean; onCancelJump?: () => void;
  header?: (items: ConversationLandmark[], jumpTo: (item: ConversationLandmark) => void) => ReactNode;
  before?: ReactNode; after?: ReactNode; footer?: ReactNode; running?: boolean; mobile?: boolean;
}) {
  const metadata = useMemo(() => {
    const values = new Map(invocations.map(item => [item.invocation_id, item]));
    if (execution && !archived) values.set(execution.current.invocation_id, execution.current);
    return values;
  }, [invocations, execution, archived]);
  const entries = useMemo(() => conversationEntries(state, [...metadata.values()]), [state.rows, metadata]);
  const rendered = useMemo(() => conversationLandmarks(state, metadata), [state.rows, state.completed, state.responses, metadata]);
  const landmarks = useMemo(() => mergeLandmarks(turnIndex, rendered), [turnIndex, rendered]);
  const { interactions, tools } = useMemo(() => conversationTools(state), [state.tools, state.interactions]);
  const area = useRef<HTMLDivElement>(null), content = useRef<HTMLDivElement>(null), dock = useRef<HTMLDivElement>(null), following = useRef(new ScrollFollowing()), touchY = useRef<number | null>(null);
  const requested = useRef<ConversationLandmark | null>(null);
  const prepend = useRef<{ anchor: Element; top: number } | null>(null);
  const [dockHeight, setDockHeight] = useState(0);
  const [jump, setJump] = useState(false);
  useLayoutEffect(() => {
    const element = area.current;
    if (!element) return;
    const update = () => {
      if (!element.clientHeight) return;
      if (following.current.following) element.scrollTo({ top: element.scrollHeight, behavior: 'instant' });
      following.current.position(element.scrollTop, element.scrollHeight, element.clientHeight);
      setJump(!following.current.following && element.scrollHeight - element.scrollTop - element.clientHeight > 8);
    };
    const observer = new ResizeObserver(() => {
      if (!element.clientHeight) return;
      setDockHeight(dock.current?.offsetHeight ?? 0);
      update();
    });
    observer.observe(element);
    if (content.current) observer.observe(content.current);
    if (dock.current) observer.observe(dock.current);
    return () => observer.disconnect();
  }, []);
  useLayoutEffect(() => {
    const element = area.current;
    if (loading || !active || !element) return;
    if (following.current.following) element.scrollTo({ top: element.scrollHeight, behavior: 'instant' });
    following.current.position(element.scrollTop, element.scrollHeight, element.clientHeight);
    setJump(!following.current.following && element.scrollHeight - element.scrollTop - element.clientHeight > 8);
  }, [state, loading, active]);
  const interrupt = () => {
    following.current.interrupt();
    const element = area.current;
    if (element && !mobile) element.scrollTo({ top: element.scrollTop, behavior: 'instant' });
  };
  const scrollToTurn = (item: ConversationLandmark, immediate = false) => {
    const root = area.current, target = document.getElementById(item.anchor);
    if (!root || !target || !root.contains(target)) return false;
    interrupt();
    target.focus({ preventScroll: true });
    root.scrollTo({ top: root.scrollTop + target.getBoundingClientRect().top - root.getBoundingClientRect().top - 16,
      behavior: immediate || window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'instant' : 'smooth' });
    return true;
  };
  useLayoutEffect(() => {
    if (requested.current && scrollToTurn(requested.current, true)) requested.current = null;
  }, [entries, loading]);
  useLayoutEffect(() => {
    const root = area.current, saved = prepend.current;
    if (loadingOlder || !root || !saved) return;
    prepend.current = null;
    if (root.contains(saved.anchor)) root.scrollTop += saved.anchor.getBoundingClientRect().top - saved.top;
  }, [entries, loadingOlder]);
  const jumpTo = (item: ConversationLandmark) => {
    prepend.current = null;
    requested.current = null;
    if (scrollToTurn(item)) {
      onCancelJump?.();
      return;
    }
    requested.current = item;
    interrupt();
    onLoadTurn?.(item.invocation);
  };
  return <>{header?.(loading ? [] : landmarks, jumpTo)}<div className={`agent-feed-shell ${!mobile && !loading && landmarks.length > 1 ? 'has-turn-navigator' : ''}`} style={{ '--agent-dock-height': `${dockHeight}px` } as CSSProperties}><div ref={area} className="agent-chat-scroll" tabIndex={0} aria-label="Chat history"
    onWheelCapture={event => { if (event.deltaY < 0) interrupt(); }}
    onTouchStartCapture={event => { touchY.current = event.touches[0]?.clientY ?? null; }}
    onTouchMoveCapture={event => { const y = event.touches[0]?.clientY; if (y !== undefined && touchY.current !== null && y > touchY.current) interrupt(); touchY.current = y ?? null; }}
    onPointerDown={event => { if (event.target === area.current) interrupt(); }}
    onKeyDownCapture={event => { if ((event.target as Element).closest('input,textarea,select,[contenteditable=true]')) return; if (['ArrowUp', 'PageUp', 'Home'].includes(event.key) || event.key === ' ' && event.shiftKey) interrupt(); }}
    onScroll={() => {
      const element = area.current!;
      if (loadingOlder && prepend.current) prepend.current.top = prepend.current.anchor.getBoundingClientRect().top;
      setJump(following.current.record(element.scrollTop, element.scrollHeight, element.clientHeight));
    }}>
    {before}
    {onLoadOlder ? <button type="button" className="agent-load-older" disabled={loadingOlder || switchingWindow} onClick={() => {
      const anchor = content.current?.firstElementChild;
      if (anchor) prepend.current = { anchor, top: anchor.getBoundingClientRect().top };
      interrupt();
      onLoadOlder();
    }}><BusyLabel busy={loadingOlder} label="Loading older turns">Load older turns</BusyLabel></button> : null}
    <div ref={content} className={`agent-feed ${!loading && !error && !entries.length ? 'is-empty' : ''}`} role="log" aria-label="Conversation" aria-live="off" aria-busy={loading}>
    {loading ? <ConversationSkeleton /> : null}
    {error ? <div className="agent-error-card" role="alert">Could not load saved messages: {error.message}<button onClick={onRetry}>Retry</button></div> : null}
    {!loading && !error && !entries.length ? <div className="agent-welcome"><h2>What would you like to work on?</h2></div> : null}
    {!loading ? entries.map(entry => {
      if (entry.type === 'message') return <ConversationMessage key={entry.id} row={entry.row} />;
      const item = metadata.get(entry.invocation);
      const hasError = entry.rows.some(row => row.type === 'error' && !row.handled);
      const status = executionStatus(item?.status ?? (state.completed[entry.invocation] ? 'completed' : hasError ? 'failed' : 'running'), execution?.current.invocation_id === entry.invocation ? execution.controls : undefined);
      return <ConversationTurn mobile={mobile} session={session} loadUsage={loadUsage} onFork={onFork} key={entry.id} turn={entry} state={state} metadata={item} lifecycle={status} active={active} tools={tools} interactions={interactions} />;
    }) : null}
  </div>{after}<div ref={dock} className="agent-composer-dock">{footer}</div></div>
    {!mobile && !loading ? <ConversationNavigator items={landmarks} rendered={rendered} area={area} content={content} active={active} onJump={jumpTo} /> : null}
    {jump || archived ? <Tooltip content={running ? 'Agent is working · Go to latest messages' : 'Go to latest messages'}><button className={`agent-jump ${running ? 'is-running' : ''}`} aria-label={running ? 'Agent is working · Go to latest messages' : 'Go to latest messages'} onClick={() => {
      requested.current = null;
      prepend.current = null;
      following.current.resume();
      onLatest?.();
      if (!archived) area.current?.scrollTo({ top: area.current.scrollHeight, behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'instant' : 'smooth' });
      setJump(false);
    }}><span className={running ? 'agent-jump-activity' : ''}><AgentIcon name={running ? 'activity' : 'down'} /></span></button></Tooltip> : null}</div></>;
});

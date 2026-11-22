import { useEffect, useId, useMemo, useState } from 'react';
import { useInfiniteQuery } from '@tanstack/react-query';
import { AnimatedRegion, AgentDisclosure, Chevron } from './AgentDisclosure.js';
import { AgentIcon } from './AgentIcon.js';
import { ConversationTools, type ConversationTool } from './ConversationTools.js';
import { applyEvents, conversationTools, emptyConversation, entityKey, sortedRows, type ConversationState, type FeedRow } from './conversationState.js';
import { finalResponse, formatElapsed, hasLogContent, isAnswerBlock, type ConversationTurn as Turn } from './conversationTurns.js';
import { Markdown, MessageContent } from './MessageContent.js';
import { CopyIcon, CheckIcon } from '../components/ToolButton.js';
import { UsageButton, type UsageLoader } from './UsageButton.js';
import { Tooltip } from '../components/Tooltip.js';
import type { AgentInvocation, Interaction, ResponseBlock } from './types.js';
import { logDisclosure, type LogChoice } from './logDisclosure.js';
import { messageAnchor } from './conversationLandmarks.js';
import { ForkButton, type ForkHandler } from './ForkButton.js';
import { agentClient } from './client.js';
import { BusyLabel, LoadingIndicator } from '../components/LoadingState.js';
import { retryAgentRead, agentReadRetryDelay } from './readFeedback.js';

export const formatTokens = (value: number | null | undefined) => value == null ? '—' : new Intl.NumberFormat('en', { notation: value >= 10000 ? 'compact' : 'standard', maximumFractionDigits: 1 }).format(value);

export function ConversationMessage({ row }: { row: Extract<FeedRow, { type: 'message' }> }) {
  if (row.message.source === 'agent') return <AgentDisclosure className="agent-tool-group agent-agent-message"
    label={`Message from Agent${row.message.sender_id ? ' ' + row.message.sender_id.slice(0, 8) : ''}`}
    icon={<AgentIcon name="message" />} status={row.message.reply_requested ? 'Reply requested' : undefined}>
    {() => <div className="agent-reasoning-body"><MessageContent content={row.message.content} /></div>}
  </AgentDisclosure>;
  return <article id={messageAnchor(row.id)} data-conversation-message={row.id} tabIndex={-1} className="agent-message is-user"><MessageContent content={row.message.content} /></article>;
}
function Reasoning({ text, running }: { text: string; running?: boolean }) {
  if (!text.trim()) return null;
  return <AgentDisclosure className="agent-reasoning" label={running ? 'Thinking…' : 'Reasoning'} icon={<AgentIcon name="sparkles" />}><div className="agent-reasoning-body"><Markdown text={text} streaming={running} /></div></AgentDisclosure>;
}
function AnswerBlocks({ blocks }: { blocks: ResponseBlock[] }) {
  return <>{blocks.map((block, index) => block.type === 'media' ? <MessageContent key={index} content={[block.content]} /> : block.type === 'text' || block.type === 'refusal' ? <Markdown key={index} text={block.text} /> : null)}</>;
}
function ResponseLog({ row, state, finalId, tools, interactions, lifecycle }: { row: Extract<FeedRow, { type: 'response' }>; state: ConversationState; finalId?: string; tools: ConversationTool[]; interactions: Map<string, Interaction>; lifecycle: string }) {
  const key = entityKey(row.invocation, row.responseId), response = state.responses[key], live = state.live[key];
  const reasoning = response ? response.blocks.filter(block => block.type === 'reasoning').map(block => block.text || block.summary.join('\n\n')).filter(Boolean).join('\n\n')
    : live?.parts.filter(part => part.kind === 'reasoning' || part.kind === 'reasoning_summary').map(part => part.text).join('\n\n') ?? '';
  const items = response ? response.blocks.filter(block => block.type === 'tool').map(block => ({ id: block.tool_id, tool: state.tools[entityKey(row.invocation, block.tool_id)], interaction: interactions.get(entityKey(row.invocation, block.tool_id)) })) : tools;
  return <div className="agent-response-log">
    <Reasoning text={reasoning} running={!response && !['failed', 'cancelled'].includes(lifecycle)} />
    {row.responseId !== finalId ? response ? <AnswerBlocks blocks={response.blocks} /> : live?.parts.filter(part => ['text', 'refusal', 'audio_transcript'].includes(part.kind)).map(part => <Markdown key={part.key} text={part.text} streaming />) : null}
    {items.length ? <ConversationTools items={items} lifecycle={lifecycle} /> : null}
    {response?.finish_reason === 'length' && row.responseId !== finalId ? <p className="agent-muted">The model reached its output limit.</p> : null}
  </div>;
}
function ProcessRow({ row, state, finalId, tools, interactions, lifecycle }: { row: FeedRow; state: ConversationState; finalId?: string; tools: Map<string, ConversationTool[]>; interactions: Map<string, Interaction>; lifecycle: string }) {
  if (row.type === 'history') return null;
  if (row.type === 'message') return <ConversationMessage row={row} />;
  if (row.type === 'error') return <div className="agent-log-error" role="note"><strong>{row.handled ? 'Handled error' : 'Attempt failed'}</strong><p>{row.error.message}</p></div>;
  if (row.type === 'compaction') {
    const compact = state.compactions[entityKey(row.invocation, row.compactionId)];
    return compact ? <div className="agent-compaction"><span />{compact.state === 'running' ? 'Compacting context…' : `Context compacted · estimated ${formatTokens(compact.before_tokens)} → ${formatTokens(compact.after_tokens)} tokens`}<span /></div> : null;
  }
  return <ResponseLog row={row} state={state} finalId={finalId} tools={tools.get(entityKey(row.invocation, row.responseId)) ?? []} interactions={interactions} lifecycle={lifecycle} />;
}
function TurnTime({ status, started, ended, active }: { status: string; started?: number | null; ended?: number | null; active: boolean }) {
  const [now, setNow] = useState(() => Date.now() * 1000);
  useEffect(() => {
    if (!active || ended != null || !['running', 'waiting', 'pausing'].includes(status) || started == null) return;
    const timer = window.setInterval(() => setNow(Date.now() * 1000), 1000);
    return () => clearInterval(timer);
  }, [active, ended, status, started]);
  const elapsed = started == null ? '' : formatElapsed((ended ?? now) - started);
  const label = ({ completed: 'Worked for', cancelled: 'Stopped', failed: 'Failed', pausing: 'Pausing', paused: 'Paused', waiting: 'Waiting', running: 'Working' } as Record<string, string>)[status] ?? 'Working';
  return <span className="agent-turn-time">{label}{elapsed ? status === 'completed' ? ` ${elapsed}` : ` · ${elapsed}` : ''}</span>;
}
function CopyResponse({ blocks }: { blocks: ResponseBlock[] }) {
  const [copied, setCopied] = useState(false), [error, setError] = useState('');
  useEffect(() => { if (!copied) return; const timer = setTimeout(() => setCopied(false), 2000); return () => clearTimeout(timer); }, [copied]);
  const copy = async () => {
    const text = blocks.flatMap(block => block.type === 'text' || block.type === 'refusal' ? [block.text] : []).join('\n\n');
    try {
      if (navigator.clipboard?.writeText) await navigator.clipboard.writeText(text);
      else {
        const field = document.createElement('textarea'); field.value = text; field.className = 'agent-copy-buffer';
        const previous = document.activeElement as HTMLElement | null; document.body.appendChild(field); field.select();
        try { if (!document.execCommand('copy')) throw new Error('Clipboard access is unavailable. Select the response text to copy it.'); }
        finally { field.remove(); previous?.focus({ preventScroll: true }); }
      }
      setCopied(true); setError('');
    } catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
  };
  return <><Tooltip content={copied ? 'Copied' : 'Copy response'}><button className="agent-answer-copy" type="button" aria-label={copied ? 'Copied' : 'Copy response'} onClick={() => void copy()}>{copied ? <CheckIcon /> : <CopyIcon />}</button></Tooltip>{error ? <span role="status" className="agent-copy-error">{error}</span> : null}</>;
}

export function ConversationTurn({ turn, state, metadata, lifecycle, active, tools, interactions, session, loadUsage, mobile, onFork }: { onFork?: ForkHandler; session: string; mobile?: boolean; loadUsage?: UsageLoader; turn: Turn; state: ConversationState; metadata?: AgentInvocation; lifecycle: string; active: boolean; tools: Map<string, ConversationTool[]>; interactions: Map<string, Interaction> }) {
  const answer = finalResponse(state, turn.invocation, lifecycle), complete = state.completed[turn.invocation];
  const finalId = answer?.response_id;
  const [choice, setChoice] = useState<LogChoice>();
  const historical = turn.rows.some(row => row.type === 'history');
  const { phase, open } = logDisclosure(finalId, choice, historical);
  const history = useInfiniteQuery({
    queryKey: ['agent-turn-details', session, turn.invocation],
    queryFn: ({ pageParam, signal }) => agentClient.historyPage(turn.invocation, pageParam, signal),
    initialPageParam: 0, getNextPageParam: page => page.next_after,
    enabled: active && historical && open, staleTime: Infinity,
    retry: retryAgentRead, retryDelay: agentReadRetryDelay,
  });
  const pages = historical ? history.data?.pages : undefined;
  const log = useMemo(() => {
    if (!pages) return { rows: turn.rows, state, tools, interactions };
    const detail = applyEvents(emptyConversation(), pages.flatMap(page => page.items));
    const rows: Record<string, FeedRow> = Object.fromEntries(turn.rows.filter(row => row.type !== 'history').map(row => [row.id, row]));
    // User messages and their jump anchors come from the summary, even during paging.
    for (const row of Object.values(detail.rows)) {
      if (row.type !== 'message' || row.message.source !== 'user') rows[row.id] = row;
    }
    const merged = { ...detail, rows,
      responses: { ...state.responses, ...detail.responses },
      tools: { ...state.tools, ...detail.tools }, interactions: { ...state.interactions, ...detail.interactions },
      compactions: { ...state.compactions, ...detail.compactions } };
    return { rows: sortedRows(merged), state: merged, ...conversationTools(merged) };
  }, [pages, turn.rows, state, tools, interactions]);
  const id = useId();
  const segments: (FeedRow[] | Extract<FeedRow, { type: 'message' }>)[] = [];
  for (const row of log.rows) {
    if (row.type === 'message' && row.message.source === 'user') { segments.push(row); continue; }
    if (!hasLogContent(row, log.state, finalId)) continue;
    const last = segments.at(-1);
    if (Array.isArray(last)) last.push(row); else segments.push([row]);
  }
  const logIds = segments.flatMap((segment, index) => Array.isArray(segment) ? [`${id}-${index}`] : []);
  if (historical) logIds.push(`${id}-history`);
  const status = answer ? 'completed' : lifecycle;
  const ended = ['completed', 'failed', 'cancelled'].includes(status) ? metadata?.completed_at_us ?? complete?.at : null;
  const started = metadata?.started_at_us ?? turn.rows.at(0)?.at;
  return <article className="agent-turn" aria-label="Agent response" data-invocation={turn.invocation}>
    <div className="agent-turn-heading">{logIds.length ? <button className="agent-turn-toggle" type="button" aria-expanded={open} aria-controls={logIds.join(' ')} onClick={() => setChoice({ phase, open: !open })}><TurnTime status={status} started={started} ended={ended} active={active} /><Chevron /></button> : <TurnTime status={status} started={started} ended={ended} active={active} />}</div>
    {segments.map((segment, index) => Array.isArray(segment) ? <AnimatedRegion key={`${id}-${index}`} id={`${id}-${index}`} open={open} className="agent-turn-log">{() => <div className="agent-process-content">{segment.map(row => row.type === 'history' && history.isPending
      ? <LoadingIndicator key={row.id} small label="Loading execution details" />
      : <ProcessRow key={row.id} row={row} state={log.state} finalId={finalId} tools={log.tools} interactions={log.interactions} lifecycle={lifecycle} />)}</div>}</AnimatedRegion> : <ConversationMessage key={segment.id} row={segment} />)}
    {historical ? <div id={`${id}-history`} hidden={!open}>
      {history.error ? <div className="agent-inline-error" role="alert">Could not load execution details.<button type="button" onClick={() => void (history.isFetchNextPageError ? history.fetchNextPage() : history.refetch())}>Retry</button></div> : null}
      {history.hasNextPage && !history.isFetchNextPageError ? <button className="agent-load-older" type="button" disabled={history.isFetchingNextPage} onClick={() => void history.fetchNextPage()}><BusyLabel busy={history.isFetchingNextPage} label="Loading execution details">Load more details</BusyLabel></button> : null}
    </div> : null}
    {answer ? <div className="agent-final-response"><AnswerBlocks blocks={answer.blocks.filter(isAnswerBlock)} />{answer.finish_reason === 'length' ? <p className="agent-muted">The model reached its output limit.</p> : null}<div className="agent-answer-actions">{answer.blocks.some(block => (block.type === 'text' || block.type === 'refusal') && block.text.trim()) ? <CopyResponse blocks={answer.blocks} /> : null}<UsageButton mobile={mobile} session={session} invocation={turn.invocation} load={loadUsage} active={active} />{onFork && lifecycle === 'completed' ? <ForkButton invocation={turn.invocation} onFork={onFork} /> : null}</div></div> : null}
    {!answer && (lifecycle === 'failed' || lifecycle === 'cancelled') ? <div className={`agent-turn-notice is-${lifecycle}`} role="status"><AgentIcon name={lifecycle === 'failed' ? 'alert' : 'stop'} /><span>{lifecycle === 'failed' ? 'Task failed. No final response was produced.' : 'You stopped this task. No final response was produced.'}</span></div> : null}
    {!answer && lifecycle === 'completed' ? <p className="agent-muted">{complete && !state.responses[entityKey(turn.invocation, complete.final_response_id)] ? 'Final response is being synchronized.' : 'Finished without a final response.'}</p> : null}
  </article>;
}

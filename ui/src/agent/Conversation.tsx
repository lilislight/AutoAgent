import { memo, useEffect, useMemo, useRef, useState } from 'react';
import { entityKey, sortedRows, type ConversationState, type FeedRow } from './conversationState';
import { Markdown, MessageContent } from './MessageContent';
import type { Interaction, ToolSnapshot } from './types';
import { AgentIcon } from './AgentIcon';
import { ConversationSkeleton } from '../components/LoadingState';

export const formatTokens = (value: number | null | undefined) => value == null ? '—' : new Intl.NumberFormat('en', { notation: value >= 10000 ? 'compact' : 'standard', maximumFractionDigits: 1 }).format(value);
function ToolCard({ tool, interaction }: { tool?: ToolSnapshot; interaction?: Interaction }) {
  if (interaction?.type === 'question' && interaction.state === 'resolved') {
    const questions = interaction.request.questions as unknown as { id: string; text: string; options?: { id: string; label: string }[] }[];
    const response = interaction.resolution?.response as unknown as { answers?: { question_id: string; text: string | null; status: string; selected_option_ids: string[] }[] } | null;
    return <details className="agent-tool agent-question-record"><summary><span className="agent-status-dot" />Your answers<span className="agent-pill">{interaction.resolution?.outcome}</span></summary><div className="agent-tool-body">{questions.map(question => {
      const answer = response?.answers?.find(item => item.question_id === question.id);
      return <div key={question.id}><strong>{question.text}</strong><p>{answer?.status === 'skipped' ? 'Skipped' : answer ? [answer.text, ...(question.options ?? []).filter(option => answer.selected_option_ids.includes(option.id)).map(option => option.label)].filter(Boolean).join(' · ') : interaction.resolution?.outcome}</p></div>;
    })}</div></details>;
  }
  if (interaction?.type === 'question') return <p className="agent-muted">Waiting for your answer</p>;
  if (!tool) return <div className="agent-tool-placeholder">Tool call · No saved result</div>;
  return <details className={`agent-tool is-${tool.state}`}><summary><span className="agent-status-dot" /><strong>{tool.name.replaceAll('_', ' ')}</strong>{tool.state !== 'completed' ? <span className="agent-pill">{tool.state}</span> : null}</summary><div className="agent-tool-body">
    <div className="agent-label">Arguments</div><pre>{pretty(tool.arguments)}</pre>
    {tool.result ? <><div className="agent-label">Result</div><MessageContent content={tool.result.content} />{tool.result.structured_content !== null ? tool.result.content.length ? <details><summary>Structured result</summary><pre>{JSON.stringify(tool.result.structured_content, null, 2)}</pre></details> : <pre>{JSON.stringify(tool.result.structured_content, null, 2)}</pre> : null}</> : <p className="agent-muted">{tool.state === 'waiting' ? 'Waiting for a message, an Agent update, or the deadline.' : 'The result will appear here when the call completes.'}</p>}
  </div></details>;
}
function pretty(value: string) { try { return JSON.stringify(JSON.parse(value), null, 2); } catch { return value; } }
const Row = memo(function Row({ row, state, interactionByTool }: { row: FeedRow; state: ConversationState; interactionByTool: Map<string, Interaction> }) {
  if (row.type === 'message') return <article className={`agent-message is-${row.message.source}`}><div className="agent-message-label">{row.message.source === 'agent' ? `Message from Agent ${row.message.sender_id?.slice(0, 8) ?? ''}` : 'You'}{row.message.reply_requested ? ' · Reply requested' : ''}</div><MessageContent content={row.message.content} /></article>;
  if (row.type === 'error') return <article className="agent-error-card" role="note"><strong>{row.handled ? 'Handled error' : 'Attempt failed'}</strong><p>{row.error.message}</p><small>{row.error.type}</small></article>;
  if (row.type === 'compaction') {
    const compact = state.compactions[entityKey(row.invocation, row.compactionId)];
    return compact ? <div className="agent-compaction"><span />{compact.state === 'running' ? 'Compacting context…' : `Context compacted · estimated ${formatTokens(compact.before_tokens)} → ${formatTokens(compact.after_tokens)} tokens`}<span /></div> : null;
  }
  const id = entityKey(row.invocation, row.responseId), response = state.responses[id], live = state.live[id];
  if (response && !response.blocks.length) return null;
  const liveTools = response ? [] : Object.entries(state.tools).filter(([key, tool]) => key === entityKey(row.invocation, tool.tool_id) && tool.response_id === row.responseId).map(([, tool]) => tool);
  return <article className="agent-message is-assistant"><div className="agent-message-label"><span className="agent-avatar">✦</span>Assistant{response ? <span className="agent-model-label">{response.model}</span> : <span className="agent-muted">Working…</span>}</div>
    {response ? response.blocks.map((block, index) => block.type === 'tool' ? <ToolCard key={block.tool_id} tool={state.tools[entityKey(row.invocation, block.tool_id)]} interaction={interactionByTool.get(entityKey(row.invocation, block.tool_id))} />
      : block.type === 'reasoning' ? <details className="agent-reasoning" key={index}><summary>Reasoning</summary><Markdown text={block.text ?? block.summary.join('\n\n')} /></details>
        : block.type === 'media' ? <MessageContent key={index} content={[block.content]} /> : <Markdown key={index} text={block.text} />)
      : <>{live?.parts.map(part => part.kind === 'reasoning' || part.kind === 'reasoning_summary' ? <details className="agent-reasoning" key={part.key}><summary>Thinking…</summary><Markdown text={part.text} /></details> : part.kind === 'text' || part.kind === 'refusal' || part.kind === 'audio_transcript' ? <Markdown key={part.key} text={part.text} /> : null)}{liveTools.map(tool => <ToolCard key={tool.tool_id} tool={tool} />)}</>}
    {response?.finish_reason === 'length' ? <p className="agent-muted">The model reached its output limit.</p> : null}
  </article>;
}, (before, after) => {
  if (before.row !== after.row) return false;
  const row = after.row;
  if (row.type === 'response') {
    const id = entityKey(row.invocation, row.responseId);
    const response = after.state.responses[id];
    if (!response || before.state.responses[id] !== response || before.state.live[id] !== after.state.live[id]) return false;
    return response.blocks.every(block => block.type !== 'tool' || (
      before.state.tools[entityKey(row.invocation, block.tool_id)] === after.state.tools[entityKey(row.invocation, block.tool_id)] &&
      before.interactionByTool.get(entityKey(row.invocation, block.tool_id)) === after.interactionByTool.get(entityKey(row.invocation, block.tool_id))));
  }
  if (row.type === 'compaction') {
    const id = entityKey(row.invocation, row.compactionId);
    return before.state.compactions[id] === after.state.compactions[id];
  }
  return true;
});
export function Conversation({ state, loading, error, onRetry, onSuggest }: { state: ConversationState; loading: boolean; error?: Error | null; onRetry: () => void; onSuggest?: (text: string) => void }) {
  const rows = useMemo(() => sortedRows(state), [state]);
  const interactions = useMemo(() => new Map(Object.entries(state.interactions).flatMap(([key, value]) => {
    const iid = (JSON.parse(key) as string[])[0]; return value.tool_ids.map(id => [entityKey(iid, id), value] as const);
  })), [state.interactions]);
  const area = useRef<HTMLDivElement>(null), follow = useRef(true);
  const [jump, setJump] = useState(false);
  useEffect(() => { if (loading) return; if (follow.current) area.current?.scrollTo({ top: area.current.scrollHeight }); else setJump(true); }, [state, loading]);
  return <div className="agent-feed-shell"><div ref={area} className={`agent-feed ${!loading && !error && !rows.length ? 'is-empty' : ''}`} role="log" aria-label="Conversation" aria-live="off" aria-busy={loading} onScroll={() => { const el = area.current!; follow.current = el.scrollHeight - el.scrollTop - el.clientHeight < 100; setJump(!follow.current); }}>
    {loading ? <ConversationSkeleton /> : null}
    {error ? <div className="agent-error-card" role="alert">Could not load saved messages: {error.message}<button onClick={onRetry}>Retry</button></div> : null}
    {!loading && !error && !rows.length ? <div className="agent-welcome"><span className="agent-welcome-symbol"><AgentIcon name="sparkles" /></span><h2>What would you like to work on?</h2><p>Research a question, work through a plan, or create something together.</p>{onSuggest ? <div className="agent-suggestions">{[['Research a topic', 'Help me research '], ['Make a plan', 'Help me make a plan for '], ['Create something', 'Help me create ']].map(([label, text]) => <button key={label} onClick={() => onSuggest(text)}>{label}</button>)}</div> : null}</div> : null}
    {!loading ? rows.map(row => <Row key={row.id} row={row} state={state} interactionByTool={interactions} />) : null}
  </div>{jump ? <button className="agent-jump" onClick={() => { follow.current = true; area.current?.scrollTo({ top: area.current.scrollHeight, behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'instant' : 'smooth' }); setJump(false); }}>↓ Latest messages</button> : null}</div>;
}

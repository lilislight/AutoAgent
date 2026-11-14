import { useCallback, useEffect, useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { FactsSkeleton } from '../components/LoadingState.js';
import { Tooltip } from '../components/Tooltip.js';
import { AgentIcon } from './AgentIcon.js';
import { AgentPopover } from './AgentPopover.js';
import type { Usage } from './types.js';

export type UsageLoader = (invocation: string, signal: AbortSignal) => Promise<{ invocation: Usage | null }>;
const count = (value: number | null | undefined) => value == null ? '—' : new Intl.NumberFormat('en').format(value);

export function UsageFacts({ usage }: { usage: Usage }) {
  const priced = usage.cost_usd !== null && (usage.call_count === 0 || usage.missing_cost_calls < usage.call_count);
  return <><dl className="agent-usage-facts">
    <div><dt>Model calls</dt><dd>{count(usage.call_count)}</dd></div>
    <div><dt>Input tokens</dt><dd>{count(usage.input_tokens)}</dd></div>
    {usage.cache_read_tokens != null && usage.cache_read_tokens > 0 ? <div className="agent-usage-subset"><dt>Cached input </dt><dd>{count(usage.cache_read_tokens)}</dd></div> : null}
    <div><dt>Output tokens</dt><dd>{count(usage.output_tokens)}</dd></div>
    {usage.reasoning_tokens != null && usage.reasoning_tokens > 0 ? <div className="agent-usage-subset"><dt>Reasoning </dt><dd>{count(usage.reasoning_tokens)}</dd></div> : null}
    <div className="agent-usage-total"><dt>Total tokens</dt><dd>{count(usage.total_tokens)}</dd></div>
    {priced ? <div><dt>{usage.missing_cost_calls || usage.pending_calls || usage.unsettled_calls ? 'Recorded cost (USD)' : 'Cost (USD)'}</dt><dd>${usage.cost_usd}</dd></div> : null}
  </dl>
    {usage.pending_calls ? <p className="agent-usage-note">{count(usage.pending_calls)} calls in progress.</p> : null}
    {usage.unsettled_calls ? <p className="agent-usage-note">Usage is unconfirmed for {count(usage.unsettled_calls)} calls.</p> : null}
    {usage.missing_usage_calls ? <p className="agent-usage-note">Token usage is missing for {count(usage.missing_usage_calls)} calls.</p> : null}
    {usage.missing_cost_calls ? <p className="agent-usage-note">Pricing is unavailable for {count(usage.missing_cost_calls)} calls.{priced ? ' The cost shown is partial.' : ''}</p> : null}
  </>;
}
function UsageContent({ invocation, session, load }: { invocation: string; session: string; load: UsageLoader }) {
  const query = useQuery({ queryKey: ['agent-usage', session, invocation], queryFn: ({ signal }) => load(invocation, signal), staleTime: 0, retry: false });
  return <>
    {query.isLoading ? <FactsSkeleton label="Loading turn usage" labels={['Model calls', 'Input tokens', 'Output tokens', 'Total tokens']} /> : null}
    {query.error ? <p className="agent-usage-error" role="alert">Could not load usage: {query.error.message}<button type="button" onClick={() => void query.refetch()}>Retry</button></p> : null}
    {query.data?.invocation ? <UsageFacts usage={query.data.invocation} /> : query.isSuccess ? <p className="agent-usage-note">No usage recorded.</p> : null}
  </>;
}
export function UsageButton({ invocation, session, load, active }: { invocation: string; session: string; load?: UsageLoader; active: boolean }) {
  const [open, setOpen] = useState(false), trigger = useRef<HTMLButtonElement>(null);
  const close = useCallback(() => setOpen(false), []);
  useEffect(() => { if (!active) setOpen(false); }, [active]);
  return <><Tooltip content="View usage for this turn" disabled={open}><button ref={trigger} type="button" className="agent-answer-copy" aria-label="View usage for this turn" aria-expanded={open} aria-haspopup="dialog" disabled={!load} onClick={() => setOpen(value => !value)}><AgentIcon name="usage" /></button></Tooltip>
    {open && active && load ? <AgentPopover anchor={trigger} label="Turn usage" onClose={close}><UsageContent invocation={invocation} session={session} load={load} /></AgentPopover> : null}
  </>;
}

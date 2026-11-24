import { useCallback, useEffect, useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { AgentIcon } from './AgentIcon.js';
import { AgentPopover } from './AgentPopover.js';
import { agentClient } from './client.js';
import { retryAgentRead, agentReadRetryDelay } from './readFeedback.js';
import type { TaskPlan } from './types.js';

export function SessionPlan({ session, active, existing, mobile }: {
  session: string; active: boolean; existing: boolean; mobile: boolean;
}) {
  const trigger = useRef<HTMLButtonElement>(null);
  const [open, setOpen] = useState(false);
  const close = useCallback(() => setOpen(false), []);
  const query = useQuery({ queryKey: ['agent-plan', session],
    queryFn: ({ signal }) => agentClient.plan(session, signal), enabled: active && existing,
    retry: retryAgentRead, retryDelay: agentReadRetryDelay });
  const plan = query.data?.plan;
  useEffect(() => { if (!active || !plan) setOpen(false); }, [active, plan]);
  useEffect(close, [session, close]);
  if (!existing) return null;
  if (!plan && query.isPending && query.fetchStatus !== 'idle') return <span className="agent-plan-placeholder skeleton" aria-label="Loading plan" />;
  if (!plan && query.error) return <button type="button" className="agent-plan-trigger" onClick={() => void query.refetch()}>Retry plan</button>;
  if (!plan) return null;
  const done = plan.plan.filter(item => item.status === 'completed').length;
  const complete = done === plan.plan.length;
  return <>
    <button ref={trigger} type="button" className="agent-plan-trigger" aria-haspopup="dialog" aria-expanded={open}
      aria-label={`View plan, ${done} of ${plan.plan.length} steps completed`} onClick={() => setOpen(value => !value)}>
      <AgentIcon name={complete ? 'check' : 'task'} /><span>Plan <span className="agent-plan-count">{done}/{plan.plan.length}</span></span><AgentIcon name="chevron" />
    </button>
    {open ? <AgentPopover anchor={trigger} sheet={mobile} label="Plan" onClose={close} className="agent-plan-panel">
      <PlanDetails plan={plan} />
      {query.error ? <p role="alert" className="agent-error-text">Could not refresh plan. <button type="button" onClick={() => void query.refetch()}>Retry</button></p> : null}
    </AgentPopover> : null}
  </>;
}

export function PlanDetails({ plan }: { plan: TaskPlan }) {
  const done = plan.plan.filter(item => item.status === 'completed').length;
  const complete = done === plan.plan.length;
  return <div className="agent-plan-details">
      <div className="agent-plan-summary"><span>{complete ? 'Completed' : `${done} of ${plan.plan.length} completed`}</span></div>
      <ol className="agent-plan-steps">
        {plan.plan.map((item, index) => <li key={`${index}:${item.step}`} data-state={item.status}>
          <span className="agent-plan-status" role="img" aria-label={item.status === 'in_progress' ? 'In progress' : item.status === 'completed' ? 'Completed' : 'Pending'}>
            {item.status === 'completed' ? <AgentIcon name="check" /> : <span className="agent-plan-dot" />}
          </span><span>{item.step}</span>
        </li>)}
      </ol>
      {plan.explanation ? <p className="agent-plan-explanation">{plan.explanation}</p> : null}
  </div>;
}

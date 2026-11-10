import { useEffect, useId, useState, type ReactNode } from 'react';
import { Tooltip } from '../components/Tooltip.js';
import { AgentIcon } from './AgentIcon.js';

type DeferredContent = ReactNode | (() => ReactNode);

/** Keep visited content mounted for closing motion and retained nested choices. */
export function AnimatedRegion({ id, open, children, className = '' }: { id: string; open: boolean; children: DeferredContent; className?: string }) {
  const [visited, setVisited] = useState(open);
  useEffect(() => { if (open) setVisited(true); }, [open]);
  return <div id={id} className={`agent-collapse ${open ? 'is-open' : ''} ${className}`} inert={!open} aria-hidden={!open}>
    <div className="agent-collapse-inner">{open || visited ? typeof children === 'function' ? children() : children : null}</div>
  </div>;
}

export function Chevron() { return <span className="agent-disclosure-chevron"><AgentIcon name="chevron" /></span>; }

export function AgentDisclosure({ label, children, className = '', icon, status, tooltip }: {
  label: ReactNode; children: DeferredContent; className?: string; icon?: ReactNode; status?: ReactNode; tooltip?: string;
}) {
  const [open, setOpen] = useState(false), id = useId();
  return <div className={`agent-disclosure ${className}`}>
    <Tooltip content={tooltip} disabled={!tooltip}><button type="button" className="agent-disclosure-button" aria-expanded={open} aria-controls={id} onClick={() => setOpen(value => !value)}>
      {icon ? <span className="agent-disclosure-icon">{icon}</span> : null}<span className="agent-disclosure-label">{label}</span>{status ? <span className="agent-disclosure-status">{status}</span> : null}<Chevron />
    </button></Tooltip>
    <AnimatedRegion id={id} open={open}>{children}</AnimatedRegion>
  </div>;
}

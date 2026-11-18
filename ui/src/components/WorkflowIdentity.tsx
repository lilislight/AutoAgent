import { useCallback, useEffect, useId, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import type { WorkflowGraphData } from '../types';
import { Tooltip, ValueTip } from './Tooltip';
import { ToolButton, CopyIcon, CheckIcon } from './ToolButton';
import { copyText } from '../clipboard';

type Identity = Pick<WorkflowGraphData, 'workflow_id' | 'workflow_version' | 'workflow_revision_id'>;

export function WorkflowVersion({ version }: { version: string }) {
  return <span className="workflow-version"><span className="workflow-version-caption">Version</span><span className="workflow-version-value">{version}</span></span>;
}

export function WorkflowIdentityDetails({ workflow }: { workflow: Identity }) {
  const [feedback, setFeedback] = useState<{ value: string; failed: boolean } | null>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(() => () => clearTimeout(timer.current), []);
  const copy = async (trigger: HTMLButtonElement, value: string) => {
    clearTimeout(timer.current);
    setFeedback({ value, failed: !await copyText(value, trigger) });
    timer.current = setTimeout(() => setFeedback(null), 2000);
  };
  return <div className="workflow-identity-details">
    <dl>{[
      ['Workflow ID', workflow.workflow_id],
      ['Version', workflow.workflow_version],
      ['Revision ID', workflow.workflow_revision_id],
    ].map(([label, value]) => <div className="workflow-identity-field" key={label}>
      <dt>{label}</dt><dd><code>{value}</code><ToolButton className="workflow-identity-copy" label={`Copy ${label}`} onClick={event => void copy(event.currentTarget, value)}>{feedback?.value===value&&!feedback.failed?<CheckIcon/>:<CopyIcon/>}</ToolButton></dd>
    </div>)}</dl>
    <div className="workflow-identity-feedback" role="status">{feedback ? feedback.failed ? 'Could not copy. Select the value and copy it.' : 'Copied' : null}</div>
  </div>;
}

/** Execution headers disclose the same identifiers as the Workflow revision picker. */
export function WorkflowIdentity({ workflow, active }: { workflow: Identity; active: boolean }) {
  const [open, setOpen] = useState(false);
  const [position, setPosition] = useState({ left: 0, top: 0, height: 400 });
  const trigger = useRef<HTMLButtonElement>(null);
  const panel = useRef<HTMLDivElement>(null);
  const closeButton = useRef<HTMLButtonElement>(null);
  const id = useId();
  const close = useCallback(() => { setOpen(false); trigger.current?.focus({ preventScroll: true }); }, []);
  useEffect(() => { setOpen(false); }, [workflow.workflow_revision_id, active]);
  useLayoutEffect(() => {
    if (!open) return;
    const update = () => {
      const bounds = trigger.current?.getBoundingClientRect();
      if (!bounds) return;
      const width = Math.min(360, window.innerWidth - 24);
      setPosition({ left: Math.max(12, Math.min(bounds.left, window.innerWidth - width - 12)), top: bounds.bottom + 8, height: Math.max(0, window.innerHeight - bounds.bottom - 20) });
    };
    update();
    closeButton.current?.focus({ preventScroll: true });
    window.addEventListener('resize', update);
    window.addEventListener('scroll', update, true);
    return () => { window.removeEventListener('resize', update); window.removeEventListener('scroll', update, true); };
  }, [open]);
  useEffect(() => {
    if (!open) return;
    const dismiss = (event: PointerEvent) => {
      if (event.target instanceof Node && !trigger.current?.contains(event.target) && !panel.current?.contains(event.target)) setOpen(false);
    };
    const escape = (event: KeyboardEvent) => { if (event.key === 'Escape') { event.stopPropagation(); close(); } };
    const focusOut = (event: FocusEvent) => {
      if (event.target instanceof Node && !trigger.current?.contains(event.target) && !panel.current?.contains(event.target)) setOpen(false);
    };
    document.addEventListener('pointerdown', dismiss);
    document.addEventListener('keydown', escape);
    document.addEventListener('focusin', focusOut);
    return () => { document.removeEventListener('pointerdown', dismiss); document.removeEventListener('keydown', escape); document.removeEventListener('focusin', focusOut); };
  }, [open, close]);
  return <>
    <Tooltip disabled={open} content={<ValueTip label="Version · View definition identifiers" value={workflow.workflow_version}/>}><button ref={trigger} type="button" className="revision-trigger workflow-identity-trigger" aria-label={`Version ${workflow.workflow_version}. View definition identifiers`} aria-expanded={open} aria-haspopup="dialog" aria-controls={open ? id : undefined} onClick={() => setOpen(value => !value)}>
      <WorkflowVersion version={workflow.workflow_version} /><svg className="identity-info-icon" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" aria-hidden="true"><circle cx="10" cy="10" r="7"/><path d="M10 9v5m0-8v.2"/></svg><svg className="revision-chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="m7 10 5 5 5-5" /></svg>
    </button></Tooltip>
    {open && active ? createPortal(<div ref={panel} id={id} className="workflow-identity-popover" role="dialog" aria-label="Definition identifiers" style={{ left: position.left, top: position.top, maxHeight: position.height }}>
      <header><strong>Definition identifiers</strong><button ref={closeButton} type="button" className="icon-button" aria-label="Close definition identifiers" onClick={close}>×</button></header>
      <WorkflowIdentityDetails key={workflow.workflow_revision_id} workflow={workflow} />
    </div>, document.body) : null}
  </>;
}

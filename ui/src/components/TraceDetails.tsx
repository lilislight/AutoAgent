import { useObservationClock } from './useObservationClock';
import { useId, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { getExecutionValue, getStageValue } from "../api";
import type { TraceRow } from "../types";
import { JsonValue } from "./JsonValue";
import { CopyId } from "./InspectorFrame";
import { useExecutionNavigation } from './ExecutionNavigation';
import { elapsedLabel } from '../timeline';
import { executionTiming, isSystemCall, valueMessage } from '../inspectorModel';

export function formatTime(time?: number | null, precise = false) {
  if (time == null) return '—';
  const formatted = new Intl.DateTimeFormat('en', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', ...(precise ? { fractionalSecondDigits: 3 as const } : {}) }).format(time / 1000);
  return precise ? formatted.replace(/\.\d{3}/, `.${String(time % 1_000_000).padStart(6, '0')}`) : formatted;
}
export function duration(microseconds?: number | null) {
  if (microseconds == null) return "—";
  return elapsedLabel(microseconds);
}
export function StatusBadge({ row }: { row: Pick<TraceRow, "status" | "control_state"> }) {
  const status = row.control_state === "paused" ? "paused" : row.status ?? "planned";
  return <span className={`status-badge status-${status}`}>{status}</span>;
}
export function RowDetails({ row }: { row: TraceRow }) {
  const source=row.inherited_from as {invocation_id:string;identity:string;through_sequence:number}|null|undefined;
  const navigation = useExecutionNavigation();
  const open = row.completed_at_us == null && ['ready', 'running', 'waiting', 'settling'].includes(row.status ?? '');
  const nowUs=useObservationClock(navigation.clock,navigation.active && !navigation.replaying && open,0);
  const elapsed = row.started_at_us != null ? row.completed_at_us != null ? row.completed_at_us - row.started_at_us : open && nowUs>0 ? Math.max(0,nowUs-row.started_at_us) : null : null;
  return <><dl className="trace-facts">
    {source?<><dt>Inherited from</dt><dd><CopyId value={source.invocation_id}/></dd><dt>Source record</dt><dd><CopyId value={source.identity}/></dd><dt>Source boundary</dt><dd>Event #{source.through_sequence}</dd></>:null}
    {row.occurrence_id ? <><dt>Occurrence ID</dt><dd><CopyId value={row.occurrence_id} /></dd></> : null}
    {row.call_id ? <><dt>Call ID</dt><dd><CopyId value={row.call_id} /></dd><dt>{isSystemCall(row) ? 'Command' : 'Operator'}</dt><dd>{row.operator_id}</dd><dt>Unit index</dt><dd>{String(row.unit_index)}</dd></> : null}
    {row.phase && row.phase !== 'none' ? <><dt>Phase</dt><dd>{String(row.phase)}</dd></> : null}
    {row.ready_at_us != null ? <><dt>Ready</dt><dd>{formatTime(row.ready_at_us, true)}</dd></> : null}<dt>Started</dt><dd>{formatTime(row.started_at_us, true)}</dd><dt>Completed</dt><dd>{formatTime(row.completed_at_us, true)}</dd><dt>Total elapsed</dt><dd title="Wall time including waiting, pauses and interruptions">{open && elapsed!=null?'~ ':''}{duration(elapsed)}</dd>
    {executionTiming(row) != null ? <><dt>Processing time</dt><dd title="Measured calling and result-processing duration; separate from the recorded start-to-end interval">{duration(executionTiming(row))}</dd></> : null}
    {row.queue_duration_ns != null && (!isSystemCall(row) || row.queue_duration_ns > 0) ? <><dt>Queue time</dt><dd>{duration(row.queue_duration_ns / 1000)}</dd></> : null}
    {row.call_count != null ? <><dt>Calls</dt><dd>{row.call_count}</dd></> : null}
    {row.peak_parallelism != null ? <><dt>Peak parallelism</dt><dd title="Peak number of active processing units">{row.peak_parallelism}</dd></> : null}
    {row.recovery_attempts != null ? <><dt>Recovery attempts</dt><dd>{String(row.recovery_attempts)}</dd></> : null}
    {row.error_type ? <><dt>Error type</dt><dd>{row.error_type}</dd></> : null}{row.error_code ? <><dt>Error code</dt><dd>{row.error_code}</dd></> : null}{row.error_phase ? <><dt>Error phase</dt><dd>{String(row.error_phase)}</dd></> : null}
  </dl></>;
}
export type ValueProps = { invocationId: string; kind: string; identity: string; field: string; label?: string; description?: string; badge?: string; occurrenceId?: string; valueKey?: string };
const openValues = new Map<string, boolean>();
export function LoadedValue({ invocationId, kind, identity, field, occurrenceId, label, valueKey }: ValueProps) {
  const query = useQuery({ queryKey: ['trace-value', invocationId, valueKey ?? JSON.stringify([kind, identity, field, occurrenceId])], queryFn: ({ signal }) => kind === 'stage' ? getStageValue(invocationId, occurrenceId!, identity, field, signal) : getExecutionValue(invocationId, kind, identity, field, signal), staleTime: Infinity, gcTime: 30_000, refetchOnWindowFocus: false });
  if (query.isPending) return <p className="muted" role="status">Loading {label ?? field}…</p>;
  if (query.error) return <div className="sidebar-error"><p>{query.error.message}</p><button type="button" onClick={() => void query.refetch()}>Retry</button></div>;
  if (!query.data.available) return <p className="muted">Not recorded.</p>;
  return <JsonValue value={query.data.value} label={label ?? field} />;
}
export function LazyValue(props: ValueProps) {
  const contentId = useId();
  const key = JSON.stringify([props.invocationId, props.valueKey ?? [props.kind, props.identity, props.field, props.occurrenceId]]);
  const [open, setOpen] = useState(() => openValues.get(key) ?? false);
  const toggle = () => {
    const next = !open;
    setOpen(next);
    openValues.delete(key); openValues.set(key, next);
    while (openValues.size > 128) openValues.delete(openValues.keys().next().value!);
  };
  const title = props.label ?? props.field;
  return <section className={`lazy-value ${open ? 'is-open' : ''}`}><button type="button" className="value-heading value-toggle-row" aria-expanded={open} aria-controls={open ? contentId : undefined} aria-label={`${open ? 'Hide' : 'Load'} ${title}`} onClick={toggle}><span className="value-heading-copy"><span className="value-title">{title}</span>{props.description ? <span className="value-description">{props.description}</span> : null}</span>{props.badge ? <span className="inspector-chip">{props.badge}</span> : null}<span className="value-toggle">{open ? 'Hide' : 'Load'}</span></button>{open ? <div id={contentId} className="value-body"><LoadedValue {...props} /></div> : null}</section>;
}
export function MissingValue({ title, message }: { title: string; message: string }) {
  return <section className="missing-value"><h3>{title}</h3><p>{message}</p></section>;
}
export function RowValues({ row, kind }: { row: TraceRow; kind: "node" | "call" | "invocation" }) {
  const identity = kind === "call" ? row.call_id! : kind === "node" ? row.occurrence_id! : row.invocation_id;
  return <>{(['input', 'output'] as const).map(field => {
    const label = kind === 'node' && field === 'input' ? 'Mapped input' : field === 'input' ? 'Input' : 'Output';
    const description = kind === 'call' ? field === 'input' ? isSystemCall(row) ? 'Request passed to this command.' : 'Validated input passed to this Call.' : isSystemCall(row) ? 'Result returned by this command.' : 'Result returned by this Call.' : undefined;
    return row.values.includes(field) ? <LazyValue key={`${identity}:${field}`} invocationId={row.invocation_id} kind={kind} identity={identity} field={field} valueKey={row.value_keys?.[field]} label={label} description={description} /> : <MissingValue key={field} title={label} message={valueMessage(row, field)} />;
  })}</>;
}

import { useId, useState, type ReactNode } from 'react';
import { BusyLabel, LoadingIndicator, SkeletonText } from '../../components/LoadingState.js';
import { AgentIcon } from '../AgentIcon.js';
import type { Decision } from './types.js';
import { ApiError } from '../../http.js';

export function Toggle({ label, checked, disabled, busy, onChange }: { label: string; checked: boolean; disabled?: boolean; busy?: boolean; onChange: (value: boolean) => void }) {
  return <span className="settings-toggle-wrap">{busy ? <LoadingIndicator small label={`Saving ${label}`} /> : null}<input className="settings-toggle" type="checkbox" role="switch" aria-label={label} aria-busy={busy || undefined} checked={checked} disabled={disabled || busy} onChange={event => onChange(event.target.checked)} /></span>;
}
export function DecisionSelect({ label, value, disabled, onChange }: { label: string; value: Decision; disabled?: boolean; onChange: (value: Decision) => void }) {
  return <select className="settings-decision" aria-label={label} value={value} disabled={disabled} onChange={event => onChange(event.target.value as Decision)}>{(['allow', 'ask', 'deny'] as const).map(value => <option key={value} value={value}>{value[0].toUpperCase() + value.slice(1)}</option>)}</select>;
}
export function Fold({ title, count, actions, children, initial = true }: { title: string; count?: number; actions?: ReactNode; children: ReactNode; initial?: boolean }) {
  const [open, setOpen] = useState(initial), id = useId();
  return <section className="settings-group"><div className="settings-group-heading"><button type="button" className="settings-group-title" aria-expanded={open} aria-controls={id} onClick={() => setOpen(value => !value)}><span className={open ? 'settings-chevron open' : 'settings-chevron'}><AgentIcon name="chevron" /></span><strong>{title}</strong>{count !== undefined ? <span className="settings-count">{count}</span> : null}</button><div className="settings-row-actions">{actions}</div></div><div id={id} className={`settings-fold ${open ? 'open' : ''}`} inert={!open} aria-hidden={!open}><div>{children}</div></div></section>;
}
export function RowsSkeleton({ count = 3, metadata = false }: { count?: number; metadata?: boolean }) {
  return <div role="status" aria-label="Loading settings">{Array.from({ length: count }, (_, index) => <div className="settings-resource-row" aria-hidden="true" key={index}><span className="settings-resource-symbol loading-placeholder" /><div className="settings-row-copy"><strong><SkeletonText width="44%" /></strong><p><SkeletonText width="78%" /></p>{metadata ? <small><SkeletonText width="60%" /></small> : null}</div><span className="settings-toggle loading-placeholder" /></div>)}</div>;
}
export function Search({ value, onChange, label }: { value: string; onChange: (value: string) => void; label: string }) {
  return <label className="settings-search"><AgentIcon name="search" /><input aria-label={label} placeholder={label} value={value} onChange={event => onChange(event.target.value)} />{value ? <button type="button" aria-label="Clear search" onClick={() => onChange('')}><AgentIcon name="close" /></button> : null}</label>;
}
export function FormActions({ busy, onCancel, label = 'Save changes', disabled = false }: { busy: boolean; onCancel: () => void; label?: string; disabled?: boolean }) {
  return <footer className="settings-form-actions"><button type="button" disabled={busy} onClick={onCancel}>Cancel</button><button type="submit" className="settings-primary" disabled={busy || disabled}><BusyLabel busy={busy} label="Saving">{label}</BusyLabel></button></footer>;
}
export function FormError({ message }: { message: string | null }) { return message ? <p className="settings-error" role="alert">{message}</p> : null; }
export const errorText = (error: unknown) => error instanceof ApiError ? error.status === 409 ? `${error.message} Go back and reopen this item to load the current configuration.` : [error.message, ...error.fields.map(field => `${field.path.join('.')}: ${field.message}`)].join(' ') : error instanceof Error ? error.message : String(error);

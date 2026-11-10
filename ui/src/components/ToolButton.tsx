import type { ButtonHTMLAttributes, ReactNode, Ref } from 'react';
import { Tooltip } from './Tooltip.js';
import { LoadingIndicator } from './LoadingState.js';

export function ToolButton({ label, tooltip = label, busy = false, children, className = '', ref, ...props }: Omit<ButtonHTMLAttributes<HTMLButtonElement>, 'children'> & { label: string; tooltip?: ReactNode; busy?: boolean; children: ReactNode; ref?: Ref<HTMLButtonElement> }) {
  return <Tooltip content={tooltip}><button {...props} ref={ref} type={props.type ?? 'button'} className={`tool-button ${className}`} aria-label={label} aria-busy={busy || undefined}>{busy ? <LoadingIndicator small decorative label={label} /> : children}</button></Tooltip>;
}

export function RefreshIcon() { return <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M16 8a6 6 0 1 0 .1 4M16 3v5h-5" /></svg>; }
export function DefinitionIcon() { return <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M11 2H5a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8zM11 2v6h6M8 11l-2 2 2 2m4-4 2 2-2 2" /></svg>; }
export function CopyIcon() { return <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><rect x="7" y="7" width="10" height="10" rx="2" /><path d="M12 7V4a1 1 0 0 0-1-1H4a1 1 0 0 0-1 1v7a1 1 0 0 0 1 1h3" /></svg>; }
export function CheckIcon() { return <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="m4 10 4 4 8-8" /></svg>; }
export function PathIcon() { return <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M5 3v9a3 3 0 0 0 3 3h7m-3-3 3 3-3 3" /><circle cx="5" cy="3" r="1.5" /></svg>; }
export function SearchValueIcon() { return <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" aria-hidden="true"><circle cx="8.5" cy="8.5" r="5.5" /><path d="m13 13 4 4" /></svg>; }
export function DownloadIcon() { return <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M10 2v10m-4-4 4 4 4-4M3 13v4h14v-4" /></svg>; }
export function CompareIcon() { return <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><circle cx="5" cy="4" r="2" /><circle cx="15" cy="16" r="2" /><path d="M5 6v11m10-3V3M8 6l2-2 2 2M10 4v7a3 3 0 0 0 3 3" /></svg>; }

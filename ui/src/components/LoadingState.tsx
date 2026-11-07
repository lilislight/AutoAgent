import { Fragment, type ReactNode } from 'react';

export function LoadingIndicator({ label, small = false, decorative = false }: { label: string; small?: boolean; decorative?: boolean }) {
  return <span className={`loading-indicator ${small ? 'is-small' : ''}`} role={decorative ? undefined : 'status'} aria-label={decorative ? undefined : label} aria-hidden={decorative || undefined}>
    <span className="loading-spinner" aria-hidden="true" />
  </span>;
}

export function SkeletonText({ width = '100%' }: { width?: string }) {
  return <span className="loading-placeholder skeleton-text" style={{ width }}>{'\u00a0'}</span>;
}

export function InlineLoading({ label }: { label: string }) {
  return <span className="inline-loading" role="status"><LoadingIndicator label={label} small decorative /><span>{label}</span></span>;
}

/** Both labels reserve space, so starting an action never resizes its button. */
export function BusyLabel({ busy = false, label, children }: { busy?: boolean; label: string; children: ReactNode }) {
  return <span className={`busy-label ${busy ? 'is-busy' : ''}`} aria-busy={busy}>
    <span className="busy-label-idle" aria-hidden={busy}>{children}</span>
    <span className="busy-label-progress" aria-hidden={!busy}><LoadingIndicator label={label} small decorative />{label}</span>
  </span>;
}

export function FactsSkeleton({ label, count = 6, labels, className = 'trace-facts' }: { label: string; count?: number; labels?: string[]; className?: string }) {
  return <div role="status" aria-label={label}><dl className={`${className} skeleton-facts`} aria-hidden="true">
    {Array.from({ length: labels?.length ?? count }, (_, i) => {
      const fields = <><dt>{labels?.[i] ?? <SkeletonText width="75%" />}</dt><dd><SkeletonText width={i % 2 ? '68%' : '90%'} /></dd></>;
      return className === 'agent-stat-list' || className === 'session-times' ? <div key={i}>{fields}</div> : <Fragment key={i}>{fields}</Fragment>;
    })}
  </dl></div>;
}

export function CodeSkeleton({ label, lines = 6 }: { label: string; lines?: number }) {
  return <div className="json-tree-shell code-skeleton" role="status" aria-label={label}><div className="json-tree" aria-hidden="true">
    {Array.from({ length: lines }, (_, i) => <div className="json-tree-row" key={i} style={{ paddingLeft: i && i !== lines - 1 ? 12 : 0 }}><SkeletonText width={`${[54, 78, 64, 88][i % 4]}%`} /></div>)}
  </div></div>;
}

export function RecordsSkeleton({ label, count = 3 }: { label: string; count?: number }) {
  return <div role="status" aria-label={label}>{Array.from({ length: count }, (_, i) => <div className="inspector-record-choice skeleton-record" key={i} aria-hidden="true">
    <span className="record-kind-icon loading-placeholder" /><span className="record-choice-label"><strong><SkeletonText width="72%" /></strong><small><SkeletonText width="54%" /></small></span><span className="record-choice-meta"><SkeletonText width="48px" /></span><span className="record-choice-arrow" />
  </div>)}</div>;
}

export function OptionsSkeleton({ label, count = 4, variant = 'select' }: { label: string; count?: number; variant?: 'select' | 'revision' | 'filter' }) {
  return <div role="status" aria-label={label}>{Array.from({ length: count }, (_, i) => <div className={`skeleton-option ${variant === 'revision' ? 'revision-choice' : variant === 'filter' ? 'filter-option' : 'select-option'}`} key={i} aria-hidden="true">
    {variant === 'filter' ? <><span className="loading-placeholder skeleton-checkbox" /><SkeletonText width="72%" /></>
      : variant === 'revision' ? <><span className="revision-choice-top"><strong><SkeletonText width="80%" /></strong></span><small><SkeletonText width="55%" /></small></>
        : <span><strong><SkeletonText width={i % 2 ? '60%' : '80%'} /></strong><small><SkeletonText width="55%" /></small></span>}
  </div>)}</div>;
}

export function CardsSkeleton({ label, count = 2 }: { label: string; count?: number }) {
  return <div className="cards-skeleton" role="status" aria-label={label}>{Array.from({ length: count }, (_, i) => <div className="loading-card" key={i} aria-hidden="true"><strong><SkeletonText width="54%" /></strong><p><SkeletonText width="90%" /></p><SkeletonText width="72%" /></div>)}</div>;
}

export function TimelineSkeleton({ label = 'Loading Timeline', count = 4 }: { label?: string; count?: number }) {
  return <div className="timeline-skeleton" role="status" aria-label={label}>{Array.from({ length: count }, (_, i) => <div className="timeline-skeleton-row" key={i} aria-hidden="true"><SkeletonText width="75%" /><span className="loading-placeholder skeleton-track" /></div>)}</div>;
}

export function MetricsSkeleton({ label }: { label: string }) {
  return <div role="status" aria-label={label}><dl className="memory-cards" aria-hidden="true">{['Process RSS', 'Peak RSS', 'Host budget', 'Budget used'].map(name => <div key={name}><dt>{name}</dt><dd><SkeletonText width="55%" /></dd></div>)}</dl><div className="loading-placeholder skeleton-chart" aria-hidden="true" /></div>;
}

export function DirectorySkeleton({ label, count = 6, kind = 'workflow', matching = false }: {
  label: string; count?: number; kind?: 'workflow' | 'session' | 'invocation' | 'conversation'; matching?: boolean;
}) {
  const rowClass = kind === 'invocation' ? 'execution-item' : kind === 'workflow' ? '' : 'session-item';
  return <ul className="workflow-list directory-skeleton" role="status" aria-label={label}>
    {Array.from({ length: count }, (_, index) => <li className={kind === 'workflow' || kind === 'conversation' ? 'workflow-group' : undefined} key={index} aria-hidden="true">
      {kind === 'workflow' && index === 0 ? <div className="directory-group-label"><SkeletonText width="80px" /></div> : null}
      <div className={`workflow-item ${rowClass} directory-skeleton-row`}>
        <span className="workflow-glyph loading-placeholder" />
        <span className="workflow-item-text">{kind === 'invocation' ? <>
          <span className="invocation-row-heading"><strong><SkeletonText width="70%" /></strong><span className="status-badge loading-placeholder skeleton-status">Completed</span></span>
          <span className="invocation-row-meta"><code><SkeletonText width="8ch" /></code><span><SkeletonText width="140px" /></span></span>
        </> : <>
          <strong><SkeletonText width={index % 2 ? '60%' : '78%'} /></strong>
          <span className="workflow-item-meta"><SkeletonText width={index % 3 ? '70%' : '85%'} /></span>
          {kind === 'session' ? <><small><SkeletonText width="94%" /></small>{matching ? <small className="session-match-note"><SkeletonText width="55%" /></small> : null}</> : null}
        </>}</span>
      </div>
    </li>)}
  </ul>;
}

export function ConversationSkeleton() {
  return <div className="conversation-skeleton" role="status" aria-label="Loading conversation">
    <div className="skeleton-message is-user" aria-hidden="true"><span className="loading-placeholder" /><span className="loading-placeholder" /></div>
    <div className="skeleton-message" aria-hidden="true"><span className="loading-placeholder skeleton-author" /><span className="loading-placeholder" /><span className="loading-placeholder" /><span className="loading-placeholder" /></div>
    <div className="skeleton-message is-user" aria-hidden="true"><span className="loading-placeholder" /></div>
    <div className="skeleton-message" aria-hidden="true"><span className="loading-placeholder skeleton-author" /><span className="loading-placeholder" /><span className="loading-placeholder" /></div>
  </div>;
}


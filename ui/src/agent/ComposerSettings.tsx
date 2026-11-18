import { useEffect, useId, useLayoutEffect, useRef, useState, type CSSProperties } from 'react';
import { createPortal } from 'react-dom';
import { LoadingIndicator } from '../components/LoadingState.js';
import { AgentIcon } from './AgentIcon.js';
import { AgentSheet } from './AgentSheet.js';
import { ContextIndicator } from './ContextIndicator.js';
import type { ContextUsage } from './contextUsage.js';
import type { AgentCatalog, SessionSettings } from './types.js';

const approvals = [
  { id: 'always_ask', name: 'Always ask' },
  { id: 'policy', name: 'Default' },
  { id: 'auto_approve', name: 'Auto approve' },
] as const;

type Props = {
  settings?: SessionSettings;
  providers: NonNullable<AgentCatalog['provider_options']>;
  disabled?: boolean;
  onChange?: (settings: SessionSettings) => Promise<void>;
  contextUsage?: ContextUsage;
  contextLoading?: boolean; mobile?: boolean; active?: boolean;
};

/** The composer owns Session choices; popovers never alter a running task's model. */
export function ComposerSettings({ settings, providers, disabled, onChange, contextUsage, contextLoading, mobile = false, active = true }: Props) {
  const [open, setOpen] = useState<'model' | 'approval' | null>(null);
  const [saving, setSaving] = useState(false), [error, setError] = useState('');
  const [effort, setEffort] = useState(settings?.model.reasoning_effort ?? '');
  const [position, setPosition] = useState({ bottom: 0, left: 0 });
  const modelButton = useRef<HTMLButtonElement>(null), approvalButton = useRef<HTMLButtonElement>(null);
  const popup = useRef<HTMLDivElement>(null), id = useId();
  const selected = providers.find(provider => provider.id === settings?.model.provider)?.model_details?.find(model => model.id === settings?.model.model);
  const levels = selected?.reasoning_effort;
  const currentEffort = settings?.model.reasoning_effort ?? levels?.default ?? '';
  const effortIndex = Math.max(0, levels?.values.indexOf(effort || currentEffort) ?? 0);
  useEffect(() => setEffort(currentEffort), [currentEffort]);
  useEffect(() => { if (!active) setOpen(null); }, [active]);
  useLayoutEffect(() => {
    if (!open || mobile || !active) return;
    const trigger = open === 'model' ? modelButton.current : approvalButton.current;
    const reposition = () => {
      const rect = trigger?.getBoundingClientRect();
      if (rect) setPosition({ bottom: Math.max(8, innerHeight - rect.top + 8), left: Math.max(12, Math.min(rect.left, innerWidth - Math.min(340, innerWidth - 24) - 12)) });
    };
    reposition();
    popup.current?.querySelector<HTMLElement>('[aria-selected="true"], button')?.focus();
    const outside = (event: PointerEvent) => {
      if (!popup.current?.contains(event.target as Node) && !modelButton.current?.contains(event.target as Node) && !approvalButton.current?.contains(event.target as Node)) setOpen(null);
    };
    const keyboard = (event: KeyboardEvent) => {
      if (event.key === 'Escape') { event.stopPropagation(); setOpen(null); trigger?.focus(); }
      if (event.key === 'Tab' && popup.current) {
        const items = Array.from(popup.current.querySelectorAll<HTMLElement>('button:not(:disabled),input:not(:disabled)'));
        const first = items[0], last = items.at(-1);
        if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
        else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
      }
    };
    document.addEventListener('pointerdown', outside); document.addEventListener('keydown', keyboard);
    window.addEventListener('resize', reposition); window.addEventListener('scroll', reposition, true);
    return () => { document.removeEventListener('pointerdown', outside); document.removeEventListener('keydown', keyboard); window.removeEventListener('resize', reposition); window.removeEventListener('scroll', reposition, true); };
  }, [open, mobile, active]);
  const save = async (next: SessionSettings, close = false) => {
    if (saving || !onChange) return;
    setSaving(true); setError('');
    try { await onChange(next); if (close) { setOpen(null); (open === 'model' ? modelButton : approvalButton).current?.focus(); } }
    catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); setEffort(currentEffort); }
    finally { setSaving(false); }
  };
  const blocked = disabled || !settings || !onChange || saving;
  const saveEffort = (index: number) => {
    const value = levels?.values[index];
    if (settings && value && value !== currentEffort && !blocked) {
      void save({ ...settings, model: { ...settings.model, reasoning_effort: value } });
    }
  };
  const modelName = selected?.name ?? settings?.model.model ?? 'Select model';
  const panelLabel = open === 'model' ? 'Choose model and reasoning effort' : 'Choose approval mode';
  const options = <>
      {open === 'approval' ? approvals.map(mode => <button type="button" className="agent-composer-option" key={mode.id} disabled={saving || disabled} aria-pressed={settings?.approval_mode === mode.id} onClick={() => settings && void save({ ...settings, approval_mode: mode.id }, true)}><span><strong>{mode.name}</strong></span>{settings?.approval_mode === mode.id ? <AgentIcon name="check" /> : null}</button>) : <>
        <div className="agent-model-options" role="listbox" aria-label="Models">{providers.map(provider => <div role="group" aria-label={provider.name} key={provider.id}><div className="agent-model-group">{provider.name}</div>{provider.models.map(modelId => {
          const model = provider.model_details?.find(item => item.id === modelId);
          const active = settings?.model.provider === provider.id && settings.model.model === modelId;
          return <button role="option" aria-selected={active} type="button" className="agent-composer-option" key={modelId} disabled={saving || disabled} onClick={() => settings && void save({ ...settings, model: { provider: provider.id, model: modelId, reasoning_effort: model?.reasoning_effort?.default ?? null } })}><span className="agent-model-name">{model?.name ?? modelId}</span>{active ? <AgentIcon name="check" /> : null}</button>;
        })}</div>)}{!providers.length ? <p className="agent-popup-help">No models configured.</p> : null}</div>
        {levels ? <div className="agent-composer-effort">
          <div className="agent-effort-heading">{mobile ? <span>Reasoning effort</span> : <label htmlFor={`${id}-effort`}>Reasoning effort</label>}<strong>{levels.values[effortIndex]}</strong></div>
          {mobile ? <div className="agent-effort-buttons" role="group" aria-label="Reasoning effort">
            {levels.values.map((value, index) => <button key={value} type="button" disabled={blocked}
              aria-pressed={index === effortIndex} onClick={() => { setEffort(value); saveEffort(index); }}>{value}</button>)}
          </div> : <><div className="agent-effort-slider" style={{ '--effort-progress': effortIndex / Math.max(1, levels.values.length - 1) } as CSSProperties}>
            <div className="agent-effort-track" aria-hidden="true">
              <div className="agent-effort-rail"><span className="agent-effort-fill" /></div>
              <div className="agent-effort-ticks">{levels.values.map((value, index) => <span key={value} data-filled={index <= effortIndex} />)}</div>
              <div className="agent-effort-thumb-position"><span className="agent-effort-thumb" /></div>
            </div>
            <input id={`${id}-effort`} type="range" min={0} max={levels.values.length - 1} step={1} value={effortIndex}
              aria-valuetext={levels.values[effortIndex]} aria-disabled={saving || undefined}
              disabled={disabled || !settings || !onChange || levels.values.length < 2}
              onChange={event => { if (!blocked) setEffort(levels.values[Number(event.target.value)]); }}
              onPointerDown={event => { if (saving) event.preventDefault(); }}
              onPointerUp={event => saveEffort(Number(event.currentTarget.value))}
              onKeyDown={event => { if (saving && event.key !== 'Tab' && event.key !== 'Escape') event.preventDefault(); }}
              onKeyUp={event => { if (['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'Home', 'End', 'PageUp', 'PageDown'].includes(event.key)) saveEffort(Number(event.currentTarget.value)); }} />
          </div>
          <div className="agent-composer-effort-labels" aria-hidden="true">{levels.values.map((value, index) =>
            <span key={value} data-current={index === effortIndex}>{value}</span>)}</div></>}
        </div> : null}
      </>}
      {error ? <p className="agent-popup-error" role="alert">{error}</p> : null}
  </>;
  return <>
    <button ref={approvalButton} type="button" className="agent-approval-picker" disabled={blocked} aria-label="Approval mode" aria-expanded={open === 'approval'} aria-controls={!mobile && open === 'approval' ? id : undefined} aria-haspopup="dialog" onClick={() => { setError(''); setOpen(value => value === 'approval' ? null : 'approval'); }}><AgentIcon name="shield" /><span>{approvals.find(mode => mode.id === settings?.approval_mode)?.name ?? 'Default'}</span><AgentIcon name="chevron" /></button>
    <span className="agent-composer-spacer" />
    {!mobile && <ContextIndicator value={contextUsage} loading={contextLoading} />}
    <button ref={modelButton} type="button" className="agent-model-picker" disabled={blocked} aria-label={`Model: ${modelName}${currentEffort ? ` · ${currentEffort}` : ''}`} aria-expanded={open === 'model'} aria-controls={!mobile && open === 'model' ? id : undefined} aria-haspopup="dialog" onClick={() => { setError(''); setOpen(value => value === 'model' ? null : 'model'); }}><span>{modelName}</span>{currentEffort ? <small>{currentEffort}</small> : null}<AgentIcon name="chevron" /></button>
    {open && active && (mobile
      ? <AgentSheet anchor={open === 'model' ? modelButton : approvalButton} label={panelLabel}
        className="agent-settings-sheet" onClose={() => setOpen(null)}><div aria-busy={saving}>{options}</div></AgentSheet>
      : createPortal(<div ref={popup} id={id} className="agent-composer-popover"
        style={{ ...position, maxHeight: Math.max(1, window.innerHeight - position.bottom - 12) }} role="dialog" aria-label={panelLabel} aria-busy={saving}>
        <div className="agent-composer-popover-heading"><strong>{open === 'model' ? 'Model' : 'Approval mode'}</strong>{saving && <LoadingIndicator label="Saving settings" small />}</div>
        {options}
      </div>, document.body))}
  </>;
}

import { BusyLabel } from '../components/LoadingState';
import { useEffect, useRef, useState } from 'react';
import { agentClient } from './client';
import type { AgentResources, SessionSettings, SettingsRecord } from './types';

const modes = { policy: 'Follow tool policy', always_ask: 'Ask for every allowed call', never_ask: 'Reject calls that require approval', bypass: 'Skip Ask; Deny still blocks' };
export function Settings({ record, providers, resources, onSaved }: { record: SettingsRecord; providers: string[]; resources?: AgentResources; onSaved: () => void }) {
  const [draft, setDraft] = useState<SessionSettings>(() => structuredClone(record.settings));
  const [busy, setBusy] = useState(false), [error, setError] = useState('');
  const model = (patch: Partial<SessionSettings['model']>) => setDraft(value => ({ ...value, model: { ...value.model, ...patch } }));
  const access = (patch: Partial<SessionSettings['access']>) => setDraft(value => ({ ...value, access: { ...value.access, ...patch } }));
  const operation = useRef<AbortController | null>(null);
  useEffect(() => () => operation.current?.abort(), []);
  const save = async () => {
    operation.current = new AbortController();
    setBusy(true); setError('');
    try { await agentClient.configure(record.session_id, draft, record.version, operation.current.signal); onSaved(); }
    catch (reason) { if (!operation.current?.signal.aborted) setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setBusy(false); }
  };
  return <div className="agent-settings"><p className="agent-muted">Saved per session. Changes apply on the next start or explicit resume.</p>
    <fieldset disabled={busy}><legend>Model</legend>
      <label className="agent-field">Provider<select value={draft.model.provider} onChange={event => model({ provider: event.target.value })}>{[...new Set([...providers, draft.model.provider])].map(name => <option key={name}>{name}</option>)}</select></label>
      <label className="agent-field">Model<input placeholder="Provider default" value={draft.model.model ?? ''} onChange={event => model({ model: event.target.value || null })} /></label>
      <div className="agent-field-pair"><label className="agent-field">Max output tokens<input type="number" min={1} step={1} placeholder="Default" value={draft.model.max_output_tokens ?? ''} onChange={event => model({ max_output_tokens: event.target.value ? Number(event.target.value) : null })} /></label><label className="agent-field">Temperature<input type="number" min={0} max={2} step={0.1} placeholder="Default" value={draft.model.temperature ?? ''} onChange={event => model({ temperature: event.target.value ? Number(event.target.value) : null })} /></label></div>
      <label className="agent-choice"><input type="checkbox" checked={draft.model.streaming} onChange={event => model({ streaming: event.target.checked })} /> Stream responses</label>
    </fieldset>
    <fieldset disabled={busy}><legend>Approval & access</legend><label className="agent-field">Approval mode<select value={draft.approval_mode} onChange={event => setDraft(value => ({ ...value, approval_mode: event.target.value as SessionSettings['approval_mode'] }))}>{Object.entries(modes).map(([id, description]) => <option key={id} value={id}>{id} — {description}</option>)}</select></label>
      <label className="agent-field">Default tool access<select value={draft.access.default} onChange={event => access({ default: event.target.value as SessionSettings['access']['default'] })}>{['allow', 'ask', 'deny'].map(action => <option key={action}>{action}</option>)}</select></label>
      <p className="agent-muted">Tool policy and access rules are evaluated together. Deny always blocks.</p>
      {[...new Set([...(resources?.tools.filter(tool => tool.origin !== 'Built-in').map(tool => tool.name) ?? []), ...Object.keys(draft.access.rules).filter(name => !resources?.tools.some(tool => tool.name === name && tool.origin === 'Built-in'))])].map(name => <label className="agent-tool-rule" key={name}><span>{name}</span><select aria-label={`${name} access`} value={draft.access.rules[name] ?? ''} onChange={event => { const rules = { ...draft.access.rules }; if (event.target.value) rules[name] = event.target.value as SessionSettings['access']['default']; else delete rules[name]; access({ rules }); }}><option value="">Default</option>{['allow', 'ask', 'deny'].map(action => <option key={action}>{action}</option>)}</select></label>)}
      {draft.access.allowed_tools !== null ? <p className="agent-muted">Allowed tool list: {draft.access.allowed_tools.join(', ') || 'none'}</p> : null}
      {draft.access.policy ? <p className="agent-muted">Host permission policy: {draft.access.policy}</p> : null}
    </fieldset>
    {error ? <p role="alert" className="agent-error-text">{error}<button onClick={onSaved}>Reload settings</button></p> : null}
    <button className="agent-primary" disabled={busy || JSON.stringify(draft) === JSON.stringify(record.settings)} onClick={() => void save()}><BusyLabel busy={busy} label="Saving">Save settings</BusyLabel></button>
  </div>;
}

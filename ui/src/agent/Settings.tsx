import { BusyLabel } from '../components/LoadingState';
import { useEffect, useId, useRef, useState } from 'react';
import { agentClient } from './client';
import type { AgentResources, SessionSettings, SettingsRecord } from './types';

const modes = { always_ask: 'Always ask', policy: 'Default — follow tool policy', auto_approve: 'Auto approve — Deny still blocks' };
export function Settings({ record, providers, providerOptions = [], onSaved, onDraftSave }: { record: SettingsRecord; providers: string[]; providerOptions?: { id: string; name: string; models: string[] }[]; resources?: AgentResources; onSaved: () => void; onDraftSave?: (settings: SessionSettings) => void }) {
  const modelList = useId();
  const [draft, setDraft] = useState<SessionSettings>(() => structuredClone(record.settings));
  const [busy, setBusy] = useState(false), [error, setError] = useState('');
  const model = (patch: Partial<SessionSettings['model']>) => setDraft(value => ({ ...value, model: { ...value.model, ...patch } }));
  const operation = useRef<AbortController | null>(null);
  useEffect(() => () => operation.current?.abort(), []);
  const save = async () => {
    if (onDraftSave) { onDraftSave(draft); return; }
    operation.current = new AbortController();
    setBusy(true); setError('');
    try { await agentClient.configure(record.session_id, draft, record.version, operation.current.signal); onSaved(); }
    catch (reason) { if (!operation.current?.signal.aborted) setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setBusy(false); }
  };
  return <div className="agent-settings"><p className="agent-muted">{onDraftSave ? 'Draft settings are saved when you send the first message.' : 'Saved per session. Model changes apply to new submissions. Approval mode changes apply immediately.'}</p>
    <fieldset disabled={busy}><legend>Model</legend>
      <label className="agent-field">Provider<select value={draft.model.provider} onChange={event => model({ provider: event.target.value })}>{[...new Set([...providers, draft.model.provider])].map(name => <option key={name} value={name}>{providerOptions.find(value => value.id === name)?.name ?? name}</option>)}</select></label>
      <label className="agent-field">Model<input list={modelList} placeholder="Provider default" value={draft.model.model ?? ''} onChange={event => model({ model: event.target.value || null })} /><datalist id={modelList}>{providerOptions.find(value => value.id === draft.model.provider)?.models.map(name => <option key={name} value={name} />)}</datalist></label>
    </fieldset>
    <fieldset disabled={busy}><legend>Approval</legend><label className="agent-field">Approval mode<select value={draft.approval_mode} onChange={event => setDraft(value => ({ ...value, approval_mode: event.target.value as SessionSettings['approval_mode'] }))}>{Object.entries(modes).map(([id, description]) => <option key={id} value={id}>{description}</option>)}</select></label>
    </fieldset>
    {error ? <p role="alert" className="agent-error-text">{error}<button onClick={onSaved}>Reload settings</button></p> : null}
    <button className="agent-primary" disabled={busy || JSON.stringify(draft) === JSON.stringify(record.settings)} onClick={() => void save()}><BusyLabel busy={busy} label="Saving">{onDraftSave ? 'Apply to draft' : 'Save settings'}</BusyLabel></button>
  </div>;
}

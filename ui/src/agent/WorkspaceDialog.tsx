import { useLayoutEffect, useRef, useState, type FormEvent } from 'react';
import { BusyLabel } from '../components/LoadingState';
import { agentClient } from './client';
import type { AgentWorkspaceInfo } from './types';

export function WorkspaceDialog({ onClose, onCreated }: { onClose: () => void; onCreated: (workspace: AgentWorkspaceInfo) => void }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const operation = useRef<AbortController | null>(null);
  const [name, setName] = useState(''), [path, setPath] = useState('');
  const [createDirectory, setCreateDirectory] = useState(false);
  const [busy, setBusy] = useState(false), [error, setError] = useState('');
  useLayoutEffect(() => {
    const element = dialog.current!;
    element.showModal();
    element.querySelector<HTMLInputElement>('input')?.focus();
    return () => { operation.current?.abort(); element.close(); };
  }, []);
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (busy || !name.trim() || !path.trim()) return;
    operation.current = new AbortController(); setBusy(true); setError('');
    try {
      const workspace = await agentClient.createWorkspace({ name: name.trim(), path: path.trim(), create_directory: createDirectory }, operation.current.signal);
      onCreated(workspace);
    } catch (reason) {
      if (!operation.current.signal.aborted) setError(reason instanceof Error ? reason.message : String(reason));
    } finally { setBusy(false); }
  };
  return <dialog ref={dialog} className="agent-workspace-dialog" aria-labelledby="workspace-dialog-title" onCancel={event => { event.preventDefault(); if (!busy) onClose(); }}>
    <header><strong id="workspace-dialog-title">Add workspace</strong><button type="button" className="icon-button" aria-label="Close workspace dialog" disabled={busy} onClick={onClose}>×</button></header>
    <form onSubmit={event => void submit(event)}>
      <fieldset disabled={busy}>
        <label className="agent-field">Name<input autoFocus required maxLength={128} value={name} onChange={event => setName(event.target.value)} placeholder="My project" /></label>
        <label className="agent-field">Directory path<input required value={path} onChange={event => setPath(event.target.value)} placeholder="/path/to/project" autoComplete="off" /><small>On the machine running AutoAgent. Relative paths start at the current project.</small></label>
        <label className="agent-choice"><input type="checkbox" checked={createDirectory} onChange={event => setCreateDirectory(event.target.checked)} /><span>Create a new directory<small>Leave unchecked to use an existing directory.</small></span></label>
      </fieldset>
      {error ? <p className="agent-error-text" role="alert">{error}</p> : null}
      <footer><button type="button" disabled={busy} onClick={onClose}>Cancel</button><button className="agent-primary" disabled={busy || !name.trim() || !path.trim()} type="submit"><BusyLabel busy={busy} label="Adding workspace">Add workspace</BusyLabel></button></footer>
    </form>
  </dialog>;
}

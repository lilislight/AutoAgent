import { useId, useLayoutEffect, useRef, useState, type FormEvent } from 'react';
import { BusyLabel } from '../components/LoadingState';
import { AgentIcon } from './AgentIcon';
import { agentClient } from './client';
import type { AgentWorkspaceInfo } from './types';

export function WorkspaceDialog({ onClose, onCreated }: { onClose: () => void; onCreated: (workspace: AgentWorkspaceInfo) => void }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const operation = useRef<AbortController | null>(null);
  const nameEdited = useRef(false), pathInput = useRef<HTMLInputElement>(null), id = useId();
  const [name, setName] = useState(''), [path, setPath] = useState('');
  const [busy, setBusy] = useState(false), [error, setError] = useState('');
  useLayoutEffect(() => {
    const element = dialog.current!;
    element.showModal();
    pathInput.current?.focus();
    return () => { operation.current?.abort(); element.close(); };
  }, []);
  const changePath = (value: string) => {
    setPath(value);
    if (!nameEdited.current) setName(value.trim().replace(/[\\/]+$/, '').split(/[\\/]/).at(-1)?.slice(0, 128) ?? '');
  };
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (busy || !name.trim() || !path.trim()) return;
    operation.current = new AbortController(); setBusy(true); setError('');
    try {
      const workspace = await agentClient.createWorkspace({ name: name.trim(), path: path.trim() }, operation.current.signal);
      onCreated(workspace);
    } catch (reason) {
      if (!operation.current.signal.aborted) {
        const message = reason instanceof Error ? reason.message : String(reason);
        setError(/^Workspace '.+' is unavailable\.$/.test(message)
          ? 'This directory could not be opened. Check its path and permissions.' : message);
      }
    } finally { setBusy(false); }
  };
  return <dialog ref={dialog} className="agent-workspace-dialog" aria-labelledby={`${id}-title`} aria-describedby={`${id}-description`} onCancel={event => { event.preventDefault(); if (!busy) onClose(); }}>
    <header><div className="agent-workspace-dialog-title"><AgentIcon name="folder-add" /><h2 id={`${id}-title`}>Add workspace</h2></div><button type="button" className="agent-workspace-dialog-close" aria-label="Close workspace dialog" disabled={busy} onClick={onClose}><AgentIcon name="close" /></button></header>
    <p id={`${id}-description`} className="agent-workspace-dialog-description">Choose a directory for your files, instructions, and skills.</p>
    <form onSubmit={event => void submit(event)}>
      <fieldset className="agent-workspace-dialog-fields" disabled={busy}>
        <label className="agent-field">Directory path<input ref={pathInput} required maxLength={4096} aria-describedby={`${id}-path-help`} value={path} onChange={event => changePath(event.target.value)} placeholder="/path/to/project" autoComplete="off" spellCheck={false} /></label>
        <p id={`${id}-path-help`} className="agent-workspace-field-help">On the machine running AutoAgent. Use an absolute path, or a path relative to the current project. Missing directories are created automatically.</p>
        <label className="agent-field">Workspace name<input required maxLength={128} value={name} onChange={event => { nameEdited.current = true; setName(event.target.value); }} placeholder="My project" autoComplete="off" /></label>
      </fieldset>
      {error ? <p className="agent-error-text" role="alert">{error}</p> : null}
      <footer><button type="button" disabled={busy} onClick={onClose}>Cancel</button><button className="agent-primary" disabled={busy || !name.trim() || !path.trim()} type="submit"><BusyLabel busy={busy} label="Adding workspace">Add workspace</BusyLabel></button></footer>
    </form>
  </dialog>;
}

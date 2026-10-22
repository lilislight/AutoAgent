import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ApiError, createSessionCheckpoint, deleteCheckpoint, getCheckpointDetails, listSessionCheckpoints, setCheckpointPinned } from '../api';
import type { CheckpointDetails, CheckpointMember } from '../types';
import { CopyId, InspectorFrame, InspectorPresence } from './InspectorFrame';
import { formatTime } from './TraceDetails';

type CheckpointAction = { kind: 'create'; pinned: boolean; sequence?: number } | { kind: 'pin'; id: string; pinned: boolean } | { kind: 'delete'; id: string };

export function SessionCheckpoints({ session, inspectorHost }: { session: string; inspectorHost: HTMLElement | null }) {
  const client = useQueryClient();
  const [selected, setSelected] = useState<string | null>(null);
  const [pinOnCreate, setPinOnCreate] = useState(true);
  const [sequenceText, setSequenceText] = useState('');
  const sequence = sequenceText.trim() === '' ? undefined : Number(sequenceText);
  const validSequence = sequence === undefined || /^\d+$/.test(sequenceText.trim()) && Number.isSafeInteger(sequence) && sequence > 0;
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null);
  const [notice, setNotice] = useState<{ error: boolean; text: string; checkpointId?: string } | null>(null);
  useEffect(() => { setSequenceText(''); setSelected(null); setConfirmDelete(null); setNotice(null); }, [session]);
  const inspectorOpen = selected !== null;
  useEffect(() => {
    if (!inspectorOpen) return;
    const close = (event: KeyboardEvent) => {
      if (inspectorHost?.closest('[inert]')) return;
      if (event.key === 'Escape' && !event.defaultPrevented && !document.querySelector('[role="listbox"],dialog[open]')) {
        event.preventDefault();
        setSelected(null);
        setConfirmDelete(null);
      }
    };
    const outside = (event: PointerEvent) => {
      const target = event.target;
      if (!(target instanceof Element) || !inspectorHost?.contains(target)) return;
      if (target.closest('.inspector-presence,button,a,input,label,select,textarea,[role="tablist"],[role="separator"],[role="button"]')) return;
      setSelected(null); setConfirmDelete(null);
    };
    window.addEventListener('keydown', close);
    document.addEventListener('pointerdown', outside);
    return () => { window.removeEventListener('keydown', close); document.removeEventListener('pointerdown', outside); };
  }, [inspectorOpen, inspectorHost]);
  const list = useInfiniteQuery({
    queryKey: ['session-checkpoints', session],
    queryFn: ({ pageParam, signal }) => listSessionCheckpoints(session, pageParam, signal),
    initialPageParam: null as string | null, getNextPageParam: page => page.next_cursor,
    staleTime: 0, refetchOnWindowFocus: false,
  });
  const detail = useQuery({
    queryKey: ['checkpoint-details', selected],
    queryFn: ({ signal }) => getCheckpointDetails(selected!, signal), enabled: selected !== null,
    staleTime: 0, refetchOnWindowFocus: false, retry: false,
  });
  const mutation = useMutation({
    mutationFn: async (action: CheckpointAction) => {
      const signal = new AbortController().signal;
      if (action.kind === 'create') return createSessionCheckpoint(session, action.pinned, signal, action.sequence);
      if (action.kind === 'pin') return setCheckpointPinned(action.id, action.pinned, signal);
      await deleteCheckpoint(action.id, signal);
      return null;
    },
    onMutate: () => { setNotice(null); setConfirmDelete(null); },
    onSuccess: async (result, action) => {
      const changedId = action.kind === 'create' ? result?.checkpoint_id : action.id;
      if (changedId) await client.cancelQueries({ queryKey: ['checkpoint-details', changedId], exact: true });
      if (action.kind === 'delete') {
        setSelected(current => current === action.id ? null : current);
        client.removeQueries({ queryKey: ['checkpoint-details', action.id], exact: true });
        setNotice({ error: false, text: 'Checkpoint deleted. Runtime Events and Invocations are kept.' });
      } else if (result) {
        client.setQueryData(['checkpoint-details', result.checkpoint_id], result);
        if (action.kind === 'create') {
          setSelected(result.checkpoint_id);
          const position = result.members.find(member => member.session_id === session)?.sequence;
          setNotice({ error: false, text: `Checkpoint ready${position === undefined ? '' : ` at Event #${position}`}.`, checkpointId: result.checkpoint_id });
        } else {
          setNotice({ error: false, text: result.pinned ? 'Checkpoint pinned.' : 'Checkpoint unpinned. Automatic cleanup may remove it on a later capture.', checkpointId: result.checkpoint_id });
        }
      }
      // A capture belongs to the whole parent/child graph. Other Session lists
      // can contain the same record; discard their old cursors and counts too.
      await client.resetQueries({ queryKey: ['session-checkpoints'] });
    },
    onError: async (error, action) => {
      setNotice({ error: true, text: error.message, checkpointId: action.kind === 'create' ? undefined : action.id });
      if (error instanceof ApiError && error.status === 404 && action.kind !== 'create') {
        setSelected(current => current === action.id ? null : current);
      }
      // A timed-out capture can still finish on the Host. Re-read metadata
      // rather than treating the failed response as proof nothing was saved.
      await Promise.all([
        client.resetQueries({ queryKey: ['session-checkpoints'] }),
        client.invalidateQueries({ queryKey: ['checkpoint-details'] }),
      ]);
    },
  });
  const rows = list.data?.pages.flatMap(page => page.items) ?? [];
  const choose = (id: string) => { setSelected(id); setConfirmDelete(null); setNotice(null); };
  const busy = mutation.isPending;
  const feedback = notice && (!inspectorOpen || !notice.checkpointId || notice.checkpointId === selected) ? <p className={`checkpoint-notice ${notice.error ? 'is-error' : ''}`} role={notice.error ? 'alert' : 'status'}>{notice.text}</p> : null;
  const inspector = <InspectorPresence open={selected !== null}>
    <InspectorFrame kind="Checkpoint" title="Saved position" context={<CopyId value={selected ?? undefined} />} tabs={['Details']} onClose={() => { setSelected(null); setConfirmDelete(null); }}>
      {() => <>
        {feedback}
        {busy && (mutation.variables?.kind === 'create' || mutation.variables?.id === selected) ? <p className="muted" role="status">{mutation.variables?.kind === 'create' ? 'Creating checkpoint…' : 'Saving change…'}</p> : null}
        {detail.isPending ? <p className="muted" role="status">Loading checkpoint…</p> : null}
        {detail.error ? <div className="sidebar-error" role="alert"><p>{detail.error.message}</p><button onClick={() => void detail.refetch()}>Retry</button></div> : null}
        {detail.data ? <CheckpointMetadata checkpoint={detail.data} session={session} /> : null}
        {detail.data && !detail.error ? <section className="checkpoint-management" aria-label="Checkpoint actions">
          {confirmDelete !== detail.data.checkpoint_id ? <div className="checkpoint-actions">
            <button type="button" className="toolbar-action" disabled={busy || detail.isFetching} onClick={() => mutation.mutate({ kind: 'pin', id: detail.data.checkpoint_id, pinned: !detail.data.pinned })}>{detail.data.pinned ? 'Unpin' : 'Pin checkpoint'}</button>
            <button type="button" className="toolbar-action checkpoint-danger" disabled={busy || detail.isFetching || !detail.data.deletable} onClick={() => setConfirmDelete(detail.data.checkpoint_id)}>Delete checkpoint</button>
          </div> : null}
          {confirmDelete === detail.data.checkpoint_id ? <div className="checkpoint-delete-confirm" role="group" aria-label="Confirm checkpoint deletion">
            <p>{detail.data.pinned ? 'This checkpoint is pinned. Delete it anyway?' : 'Delete this checkpoint?'}</p><p className="muted">Runtime Events and Invocations are kept.</p>
            <div className="checkpoint-actions"><button type="button" className="toolbar-action" disabled={busy} onClick={() => setConfirmDelete(null)}>Keep checkpoint</button><button type="button" className="toolbar-action checkpoint-danger" disabled={busy || detail.isFetching || !detail.data.deletable} onClick={() => mutation.mutate({ kind: 'delete', id: detail.data.checkpoint_id })}>Confirm delete</button></div>
          </div> : null}
        </section> : null}
      </>}
    </InspectorFrame>
  </InspectorPresence>;
  return <>
    <div className="checkpoint-toolbar">
      <div><strong>{list.data ? `${list.data.pages[0].total_count} saved` : 'Checkpoints'}</strong><p className="muted">Pins are kept during automatic cleanup.</p></div>
      <div className="checkpoint-create">
        <label className="checkpoint-sequence">Event sequence<input type="text" inputMode="numeric" placeholder="Latest" aria-label="Checkpoint Event sequence" aria-invalid={!validSequence} title="Leave empty for current state, or enter this Session’s Event sequence. Captures state immediately after that Event." value={sequenceText} disabled={busy} onChange={event => setSequenceText(event.target.value)} /></label>
        <label><input type="checkbox" checked={pinOnCreate} disabled={busy} onChange={event => setPinOnCreate(event.target.checked)} />Pin on create</label><button type="button" className="toolbar-action" title="Capture the selected recoverable position without changing live execution. Identical graph positions reuse the same checkpoint." disabled={busy || !validSequence} onClick={() => mutation.mutate({ kind: 'create', pinned: pinOnCreate, sequence })}>{busy && mutation.variables?.kind === 'create' ? 'Creating…' : 'Create checkpoint'}</button>
      </div>
    </div>
    {!validSequence ? <p className="checkpoint-notice is-error" role="alert">Enter a positive Event sequence.</p> : null}
    {!inspectorOpen ? feedback : null}
    {list.isPending ? <p className="muted" role="status">Loading checkpoints…</p> : null}
    {list.error ? <div className="sidebar-error" role="alert"><p>{list.error.message}</p><button onClick={() => void list.refetch()}>Retry</button></div> : null}
    {rows.length ? <>
      <div className="checkpoint-columns" aria-hidden="true"><span>Captured</span><span>Session position</span><span>Graph</span></div>
      <ul className="checkpoint-list">{rows.map(row => {
        const member = row.members.find(value => value.session_id === session);
        return <li key={row.checkpoint_id}><button type="button" className={`checkpoint-row ${selected === row.checkpoint_id ? 'is-selected' : ''}`} aria-pressed={selected === row.checkpoint_id} onClick={() => choose(row.checkpoint_id)}>
          <span><strong>{formatTime(row.captured_at_us)}</strong><small>{reasonLabel(row.created_reason)} · <code>{row.checkpoint_id.slice(0, 8)}</code></small></span>
          <span><strong>Event #{row.sequence}</strong><small>{member?.invocation_id ? `Invocation ${member.invocation_id.slice(0, 8)}` : 'No Invocation'}</small></span>
          <span><strong>{row.members.length} {row.members.length === 1 ? 'Session' : 'Sessions'}</strong><small>{row.pinned ? 'Pinned' : 'Unpinned'}</small></span>
        </button></li>;
      })}</ul>
    </> : !list.isPending && !list.error ? <p className="muted">No checkpoints saved. Create one to capture the current recoverable position.</p> : null}
    {list.hasNextPage ? <button className="load-more" disabled={list.isFetchingNextPage || busy} onClick={() => void list.fetchNextPage()}>{list.isFetchingNextPage ? 'Loading…' : 'Show more'}</button> : null}
    {inspectorHost ? createPortal(inspector, inspectorHost) : null}
  </>;
}

function reasonLabel(reason: string) {
  return ({ automatic: 'Automatic', manual: 'Manual', unload: 'Unload', shutdown: 'Shutdown', import: 'Imported' } as Record<string, string>)[reason] ?? reason;
}

function CheckpointMetadata({ checkpoint, session }: { checkpoint: CheckpointDetails; session: string }) {
  return <>
    <dl className="trace-facts">
      <dt>Captured</dt><dd>{formatTime(checkpoint.captured_at_us, true)}</dd>
      <dt>Reason</dt><dd>{reasonLabel(checkpoint.created_reason)}</dd>
      <dt>Retention</dt><dd>{checkpoint.pinned ? 'Pinned · excluded from automatic cleanup' : 'Unpinned · automatic retention applies'}</dd>
    </dl>
    {checkpoint.delete_blockers.length ? <div className="checkpoint-protection" role="note"><strong>Required for recovery</strong>{checkpoint.delete_blockers.map(reason => <p key={reason}>{reason === 'required_anchor' ? 'This checkpoint is the recovery anchor for imported history.' : reason === 'referenced_details' ? 'Saved execution details still depend on this checkpoint.' : reason}</p>)}<p>Unpinning does not remove this protection.</p></div> : null}
    <section className="inspector-section checkpoint-members"><header><h3>Graph members</h3><span>{checkpoint.members.length}</span></header>
      {checkpoint.members.map(member => <CheckpointMemberDetails key={member.session_id} member={member} root={member.session_id === checkpoint.root_session_id} current={member.session_id === session} />)}
    </section>
  </>;
}

function CheckpointMemberDetails({ member, root, current }: { member: CheckpointMember; root: boolean; current: boolean }) {
  return <div className="checkpoint-member"><div className="checkpoint-member-heading"><strong>{root ? 'Root Session' : 'Child Session'}{current ? ' · current' : ''}</strong><span>Event #{member.sequence}</span></div><CopyId value={member.session_id} />
    <dl className="trace-facts"><dt>Invocation</dt><dd>{member.invocation_id ? <CopyId value={member.invocation_id} /> : '—'}</dd><dt>Revision</dt><dd>{member.workflow_revision_id ? <CopyId value={member.workflow_revision_id} /> : '—'}</dd></dl>
  </div>;
}

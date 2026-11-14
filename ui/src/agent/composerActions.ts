export type ComposerActionKind = 'send' | 'pause' | 'recover' | 'none';
export interface ComposerAction { kind: ComposerActionKind; label: string; disabled: boolean; working: boolean }

/** Sending a message never implies resuming a paused execution. */
export function composerAction({ draft, canSend, running, lifecycle, canPause, canResume, sending, pending }: {
  draft: string; canSend: boolean; running: boolean; lifecycle: string; canPause: boolean; canResume: boolean;
  sending: boolean; pending: 'pause' | 'recover' | 'cancel' | null;
}): ComposerAction {
  if (sending) return { kind: 'send', label: 'Sending message', disabled: true, working: true };
  if (pending) return { kind: 'none', label: pending === 'pause' ? 'Pausing task' : pending === 'cancel' ? 'Cancelling task' : 'Resuming task', disabled: true, working: true };
  if (draft.trim()) return { kind: 'send', label: running ? 'Queue message' : 'Send message', disabled: !canSend, working: false };
  if (lifecycle === 'paused' && canResume) return { kind: 'recover', label: 'Resume task', disabled: false, working: false };
  if (running && canPause) return { kind: 'pause', label: 'Pause task', disabled: false, working: false };
  if (running) return { kind: 'none', label: 'Agent running', disabled: true, working: false };
  return { kind: 'send', label: 'Send message', disabled: true, working: false };
}

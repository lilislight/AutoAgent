export function ManualCopyText({ label, text, onDismiss }: { label: string; text: string; onDismiss: () => void }) {
  return <div className="manual-copy-text">
    <p role="status">Clipboard access is unavailable. Select and copy the text below.</p>
    <textarea aria-label={label} readOnly rows={3} value={text} onFocus={event => event.currentTarget.select()} />
    <button type="button" onClick={onDismiss}>Dismiss</button>
  </div>;
}

/** Copy from a user gesture, including on plain HTTP where the Clipboard API is unavailable. */
export async function copyText(text: string, trigger: HTMLElement): Promise<boolean> {
  const document = trigger.ownerDocument;
  try {
    const clipboard = document.defaultView?.navigator.clipboard;
    if (clipboard?.writeText) {
      await clipboard.writeText(text);
      return true;
    }
  } catch {
    // Browsers can also reject the Clipboard API in a secure context.
  }

  const container = trigger.parentElement;
  if (!trigger.isConnected || !container) return false;
  const field = document.createElement('textarea');
  field.className = 'clipboard-fallback';
  field.readOnly = true;
  field.tabIndex = -1;
  field.value = text;
  // Keep the selection inside the same modal or focus-sensitive field controls.
  container.append(field);
  try {
    field.focus({ preventScroll: true });
    field.select();
    field.setSelectionRange(0, text.length);
    return document.execCommand('copy');
  } catch {
    return false;
  } finally {
    if (document.activeElement === field && trigger.isConnected) trigger.focus({ preventScroll: true });
    field.remove();
  }
}

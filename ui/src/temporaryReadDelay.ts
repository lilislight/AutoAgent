// TEMPORARY: requested for visual loading checks. Remove this file and its
// request() hook once those checks are complete. Mutations and SSE bypass it.
export function temporaryReadDelay(signal: AbortSignal): Promise<void> {
  signal.throwIfAborted();
  return new Promise((resolve, reject) => {
    const abort = () => { clearTimeout(timer); reject(signal.reason); };
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', abort);
      resolve();
    }, 1000);
    signal.addEventListener('abort', abort, { once: true });
  });
}

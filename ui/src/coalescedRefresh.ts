export interface CoalescedRefresh {
  request(): void;
  flush(): Promise<void>;
  dispose(): void;
}

export function createCoalescedRefresh(
  run: () => Promise<void>,
  onError: (reason: unknown) => void,
  delayMs = 30,
): CoalescedRefresh {
  let timer: ReturnType<typeof setTimeout> | null = null;
  let dirty = false;
  let disposed = false;
  let active: Promise<void> | null = null;

  const schedule = () => {
    if (disposed || !dirty || timer !== null || active !== null) return;
    timer = setTimeout(() => {
      timer = null;
      void execute();
    }, delayMs);
  };

  const execute = (): Promise<void> => {
    if (disposed) return Promise.resolve();
    if (active !== null) return active;
    const operation = (async () => {
      dirty = false;
      try {
        await run();
      } catch (reason) {
        if (!disposed) onError(reason);
      }
    })();
    const completion = operation.finally(() => {
      if (active === completion) {
        active = null;
        schedule();
      }
    });
    active = completion;
    return completion;
  };

  return {
    request() {
      if (disposed) return;
      dirty = true;
      schedule();
    },
    async flush() {
      if (disposed) return Promise.resolve();
      while (!disposed) {
        if (timer !== null) clearTimeout(timer);
        timer = null;
        if (active !== null) {
          await active;
          continue;
        }
        if (!dirty) return;
        await execute();
      }
    },
    dispose() {
      disposed = true;
      dirty = false;
      if (timer !== null) clearTimeout(timer);
      timer = null;
    },
  };
}

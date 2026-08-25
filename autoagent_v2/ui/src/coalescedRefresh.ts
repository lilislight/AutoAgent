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

  const execute = (): Promise<void> => {
    if (disposed) return Promise.resolve();
    if (active !== null) return active;
    const operation = (async () => {
      try {
        do {
          dirty = false;
          await run();
        } while (dirty && !disposed);
      } catch (reason) {
        onError(reason);
      }
    })();
    const completion = operation.finally(() => {
      if (active === completion) active = null;
    });
    active = completion;
    return completion;
  };

  return {
    request() {
      if (disposed) return;
      dirty = true;
      if (timer !== null || active !== null) return;
      timer = setTimeout(() => {
        timer = null;
        void execute();
      }, delayMs);
    },
    flush() {
      if (disposed) return Promise.resolve();
      if (timer !== null) clearTimeout(timer);
      timer = null;
      return dirty || active !== null ? execute() : Promise.resolve();
    },
    dispose() {
      disposed = true;
      dirty = false;
      if (timer !== null) clearTimeout(timer);
      timer = null;
    },
  };
}

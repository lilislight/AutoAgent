export type RequestToken = Readonly<{
  scope: string;
  generation: number;
}>;

/**
 * Tracks the latest asynchronous request in each independent UI scope.
 *
 * Fetch cancellation is an optimization only: a response is allowed to settle
 * after cancellation, but it may mutate the UI only while its token is current.
 */
export class RequestGate {
  private generation = 0;
  private readonly current = new Map<string, number>();
  private readonly exclusive = new Set<string>();

  start(scope: string): RequestToken {
    const token = { scope, generation: ++this.generation };
    this.current.set(scope, token.generation);
    return token;
  }

  tryStartExclusive(scope: string): RequestToken | null {
    if (this.exclusive.has(scope)) return null;
    const token = this.start(scope);
    this.exclusive.add(scope);
    return token;
  }

  isCurrent(token: RequestToken): boolean {
    return this.current.get(token.scope) === token.generation;
  }

  finish(token: RequestToken): boolean {
    if (!this.isCurrent(token)) return false;
    this.current.delete(token.scope);
    this.exclusive.delete(token.scope);
    return true;
  }

  invalidate(scope: string): void {
    this.current.delete(scope);
    this.exclusive.delete(scope);
  }
}

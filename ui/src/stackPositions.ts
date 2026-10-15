/** A bounded per-invocation UI memory containing only execution ordinals. */
export class StackPositions {
  private positions = new Map<string, number>();
  constructor(private capacity = 256) {}
  private key(invocation: string, kind: string, id: string) { return JSON.stringify([invocation, kind, id]); }
  read(invocation: string, kind: string, id: string, total: number) {
    const key = this.key(invocation, kind, id), ordinal = this.positions.get(key);
    if (ordinal === undefined) return 0;
    this.positions.delete(key); this.positions.set(key, ordinal);
    return Math.max(0, Math.min(total - 1, total - 1 - ordinal));
  }
  write(invocation: string, kind: string, id: string, total: number, position: number) {
    const key = this.key(invocation, kind, id);
    this.positions.delete(key);
    this.positions.set(key, Math.max(0, total - 1 - position));
    while (this.positions.size > this.capacity) this.positions.delete(this.positions.keys().next().value!);
  }
}

export const stackPositions = new StackPositions();

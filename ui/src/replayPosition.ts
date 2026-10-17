export type ReplayPosition = { sequence?: number; at_us?: number };
/** Small per-source bookmarks; no Replay frames or business values are retained. */
export class ReplayPositions {
  private readonly entries = new Map<string, ReplayPosition>();
  constructor(private readonly limit = 128) {}
  read(key: string): ReplayPosition | undefined {
    const value = this.entries.get(key);
    if (!value) return;
    this.entries.delete(key); this.entries.set(key, value);
    return { ...value };
  }
  write(key: string, value: ReplayPosition) {
    this.entries.delete(key); this.entries.set(key, { ...value });
    while (this.entries.size > this.limit) this.entries.delete(this.entries.keys().next().value!);
  }
}
export const replayPositions = new ReplayPositions();

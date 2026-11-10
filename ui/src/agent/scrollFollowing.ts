/** User intent stops following immediately; new output must never re-enable it. */
export class ScrollFollowing {
  following = true;
  private top = 0;
  private height = 0;
  private viewport = 0;

  interrupt() { this.following = false; }
  resume() { this.following = true; }
  record(top: number, height: number, viewport: number) {
    const stable = height === this.height && viewport === this.viewport;
    const distance = Math.max(0, height - top - viewport);
    if (stable && top < this.top - .5) this.following = false;
    else if (top > this.top + .5 && distance <= 2) this.following = true;
    this.position(top, height, viewport);
    return !this.following && distance > 8;
  }
  position(top: number, height: number, viewport: number) {
    this.top = top; this.height = height; this.viewport = viewport;
  }
}

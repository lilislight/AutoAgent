export interface LogChoice { phase: string; open: boolean }
/** A final answer starts a fresh collapsed phase, even after inspecting live logs. */
export function logDisclosure(finalId: string | undefined, choice: LogChoice | undefined) {
  const phase = finalId ? `final:${finalId}` : 'process';
  return { phase, open: choice?.phase === phase ? choice.open : !finalId };
}

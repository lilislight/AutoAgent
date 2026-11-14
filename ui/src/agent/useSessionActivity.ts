import { useCallback, useEffect, useState } from 'react';
import type { AgentSession } from './types.js';
import { loadActivity, observeSessions, readSession, type SessionActivities } from './sessionActivity.js';

export interface DirectoryActivity {
  sessions: SessionActivities;
  observe: (rows: AgentSession[]) => void;
  read: (session: string) => void;
}

/** Notifications are browser-local read state; Host execution remains authoritative. */
export function useSessionActivity(scope: string, session: string | null, active: boolean): DirectoryActivity {
  const key = `autoagent:session-activity:v1:${scope}`;
  const [sessions, setSessions] = useState(() => loadActivity(key));
  const [visible, setVisible] = useState(() => typeof document !== 'undefined' && !document.hidden);
  useEffect(() => {
    const update = () => setVisible(!document.hidden);
    document.addEventListener('visibilitychange', update);
    return () => document.removeEventListener('visibilitychange', update);
  }, []);
  const viewed = active && visible ? session : null;
  const read = useCallback((id: string) => setSessions(previous => readSession(previous, id)), []);
  const observe = useCallback((rows: AgentSession[]) => setSessions(previous => observeSessions(previous, rows, viewed)), [viewed]);
  useEffect(() => { if (viewed) read(viewed); }, [viewed, read]);
  useEffect(() => {
    try { localStorage.setItem(key, JSON.stringify({ v: 1, sessions })); }
    catch { /* Status and read receipts still work without browser storage. */ }
  }, [key, sessions]);
  return { sessions, observe, read };
}

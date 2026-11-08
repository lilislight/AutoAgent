import type { ReactNode } from 'react';

export type AgentIconName = 'new' | 'panel' | 'task' | 'tools' | 'skills' | 'settings' | 'send' | 'shield' | 'access' | 'sparkles' | 'folder';
const paths: Record<AgentIconName, ReactNode> = {
  folder: <path d="M3 7a2 2 0 0 1 2-2h5l2 2h7a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2Z" />,
  new: <><path d="M12 5H5a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-7M16 3l5 5M10 14l2-5 7-7 5 5-7 7-5 2Z" /></>,
  panel: <><rect x="3" y="4" width="18" height="16" rx="2" /><path d="M9 4v16" /></>,
  task: <><rect x="3" y="4" width="18" height="16" rx="2" /><path d="M15 4v16" /></>,
  tools: <><path d="m12 3 9 5-9 5-9-5 9-5ZM3 8v9l9 5 9-5V8M12 13v9" /></>,
  skills: <><path d="M12 5C8 2 4 3 2 4v15c3-2 6-1 10 1 4-2 7-3 10-1V4c-2-1-6-2-10 1ZM12 5v15" /></>,
  settings: <><path d="M4 6h16M4 12h16M4 18h16" /><circle cx="8" cy="6" r="2" /><circle cx="16" cy="12" r="2" /><circle cx="10" cy="18" r="2" /></>,
  send: <path d="M12 19V5m-6 6 6-6 6 6" />,
  shield: <><path d="m12 3 8 3v6c0 5-8 9-8 9s-8-4-8-9V6l8-3Z" /><path d="m8 12 3 3 5-6" /></>,
  access: <><rect x="5" y="10" width="14" height="11" rx="2" /><path d="M8 10V7a4 4 0 0 1 8 0v3M12 14v3" /></>,
  sparkles: <><path d="m12 3 2.5 6.5L21 12l-6.5 2.5L12 21l-2.5-6.5L3 12l6.5-2.5L12 3ZM20 2v4m-2-2h4" /></>,
};

export function AgentIcon({ name }: { name: AgentIconName }) {
  return <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">{paths[name]}</svg>;
}

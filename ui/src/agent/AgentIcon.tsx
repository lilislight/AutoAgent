import type { ReactNode } from 'react';

export type AgentIconName = 'folder-add' | 'new' | 'task' | 'tools' | 'skills' | 'settings' | 'send' | 'shield' | 'access' | 'sparkles' | 'folder' | 'chevron' | 'file' | 'search' | 'terminal' | 'edit' | 'agent' | 'message' | 'wrench' | 'stop' | 'alert' | 'usage' | 'pause' | 'play' | 'check' | 'down' | 'close' | 'activity' | 'link' | 'back' | 'more' | 'plus' | 'fork';
const paths: Record<AgentIconName, ReactNode> = {
  'folder-add': <><path d="M3 7a2 2 0 0 1 2-2h5l2 2h7a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2Z" /><path d="M8 14h8M12 10v8" /></>,
  fork: <><circle cx="7" cy="5" r="2" /><circle cx="17" cy="5" r="2" /><circle cx="7" cy="19" r="2" /><path d="M7 7v10M17 7v2a5 5 0 0 1-5 5H7" /></>,
  back: <path d="m15 5-7 7 7 7" />,
  more: <><circle cx="5" cy="12" r="1" /><circle cx="12" cy="12" r="1" /><circle cx="19" cy="12" r="1" /></>,
  down: <path d="M12 5v14m-6-6 6 6 6-6" />,
  close: <path d="m6 6 12 12M6 18 18 6" />,
  plus: <path d="M12 5v14M5 12h14" />,
  link: <path d="M10 13a4 4 0 0 0 6 0l4-4a4 4 0 0 0-6-6l-2 2M14 11a4 4 0 0 0-6 0l-4 4a4 4 0 0 0 6 6l2-2" />,
  activity: <><path d="M5 15V9M12 19V5M19 15V9" /></>,
  check: <path d="m5 12 4 4L19 6" />,
  pause: <><path d="M8 5v14M16 5v14" /></>,
  play: <path d="m8 4 12 8-12 8Z" />,
  usage: <><path d="M4 20V10M10 20V4M16 20v-9M22 20H2" /></>,
  chevron: <path d="m6 9 6 6 6-6" />,
  file: <><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8Z" /><path d="M14 2v6h6M8 13h8M8 17h6" /></>,
  search: <><circle cx="10.5" cy="10.5" r="7.5" /><path d="m16 16 5 5" /></>,
  terminal: <><rect x="2" y="3" width="20" height="18" rx="2" /><path d="m6 8 4 4-4 4M13 16h5" /></>,
  edit: <><path d="M12 3H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7M16 3l5 5M10 14l2-5 7-7 5 5-7 7-5 2Z" /></>,
  agent: <><rect x="4" y="7" width="16" height="14" rx="3" /><path d="M12 3v4M8 12h.01M16 12h.01M8 17h8M2 11v6M22 11v6" /></>,
  message: <path d="M21 11a8 8 0 0 1-8 8H7l-5 3V7a5 5 0 0 1 5-5h6a8 8 0 0 1 8 9Z" />,
  wrench: <path d="M14.7 6.3a5 5 0 0 0 6.5 6.5l-9.4 9.4a3 3 0 0 1-4.2-4.2L17 8.6a5 5 0 0 0-6.5-6.5l3.2 3.2-3 3-3.2-3.2a5 5 0 0 0 6.5 6.5" />,
  stop: <><circle cx="12" cy="12" r="9" /><rect x="9" y="9" width="6" height="6" rx="1" /></>,
  alert: <><circle cx="12" cy="12" r="9" /><path d="M12 7v6M12 17h.01" /></>,
  folder: <path d="M3 7a2 2 0 0 1 2-2h5l2 2h7a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2Z" />,
  new: <><path d="M12 5H5a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-7M16 3l5 5M10 14l2-5 7-7 5 5-7 7-5 2Z" /></>,
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

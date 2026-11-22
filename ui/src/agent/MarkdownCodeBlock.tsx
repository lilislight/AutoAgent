import { useEffect, useRef, useState, type ReactNode } from 'react';
import { copyText } from '../clipboard.js';
import { ManualCopyText } from '../components/ManualCopyText.js';
import { CheckIcon, CopyIcon, ToolButton } from '../components/ToolButton.js';
import { MermaidDiagram } from './MermaidDiagram.js';

export function MarkdownCodeBlock({ source, diagram, children }: { source: string; diagram: boolean; children: ReactNode }) {
  const [feedback, setFeedback] = useState<{ source: string; copied: boolean }>();
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(() => () => clearTimeout(timer.current), []);
  const current = feedback?.source === source ? feedback : undefined;
  const copy = async (trigger: HTMLButtonElement) => {
    const copied = await copyText(source, trigger);
    if (!trigger.isConnected) return;
    clearTimeout(timer.current);
    setFeedback({ source, copied });
    if (copied) timer.current = setTimeout(() => setFeedback(undefined), 1600);
  };
  const pre = <pre>{children}</pre>;
  const actions = <ToolButton className="agent-code-copy" label={diagram ? 'Copy Mermaid source' : 'Copy code'}
    tooltip={current?.copied ? 'Copied' : diagram ? 'Copy Mermaid source' : 'Copy code'} onClick={event => void copy(event.currentTarget)}>
    {current?.copied ? <CheckIcon /> : <CopyIcon />}
  </ToolButton>;
  return <div className="agent-code-block">
    {diagram ? <MermaidDiagram source={source} fallback={pre} actions={actions} /> : <><div className="agent-code-actions">{actions}</div>{pre}</>}
    {current?.copied && <span className="sr-only" role="status">Copied</span>}
    {current && !current.copied && <ManualCopyText label={diagram ? 'Mermaid source to copy' : 'Code to copy'}
      text={source} onDismiss={() => setFeedback(undefined)} />}
  </div>;
}

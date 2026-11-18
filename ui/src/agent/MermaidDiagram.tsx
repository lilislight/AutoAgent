import { createContext, useContext, useEffect, useId, useState } from 'react';
import type { Theme } from '../theme.js';

export const AgentDiagramTheme = createContext<Theme>('light');

let rendering: Promise<unknown> = Promise.resolve();

function renderDiagram(source: string, id: string, theme: Theme, signal: AbortSignal) {
  const result = rendering.then(async () => {
    if (signal.aborted) return;
    const { default: mermaid } = await import('mermaid');
    if (signal.aborted) return;

    // Mermaid shares its configuration across renders.
    mermaid.initialize({
      startOnLoad: false,
      securityLevel: 'strict',
      suppressErrorRendering: true,
      theme: theme === 'dark' ? 'dark' : 'default',
      htmlLabels: false,
      arrowMarkerAbsolute: false,
      secure: ['secure', 'securityLevel', 'startOnLoad', 'suppressErrorRendering', 'maxTextSize', 'maxEdges',
        'theme', 'themeVariables', 'themeCSS', 'htmlLabels', 'flowchart', 'fontFamily', 'dompurifyConfig', 'arrowMarkerAbsolute'],
    });

    const staging = document.createElement('div');
    staging.className = 'agent-mermaid-staging';
    staging.setAttribute('aria-hidden', 'true');
    document.body.append(staging);
    try {
      const { svg } = await mermaid.render(id, source, staging);
      if (signal.aborted) return;
      const element = new DOMParser().parseFromString(svg, 'image/svg+xml').documentElement;
      if (element.localName !== 'svg') throw new Error('Invalid diagram output.');
      const width = (element.getAttribute('viewBox') ?? '').trim().split(/\s+/).map(Number)[2];
      return {
        svg,
        width: Number.isFinite(width) && width > 0 ? Math.ceil(width) : undefined,
        alt: element.querySelector('title')?.textContent?.trim() || 'Mermaid diagram',
      };
    } finally {
      staging.remove();
    }
  });
  rendering = result.catch(() => {});
  return result;
}

export function MermaidDiagram({ source, fallback }: { source: string; fallback: React.ReactNode }) {
  const theme = useContext(AgentDiagramTheme);
  const id = 'agent-mermaid-' + useId().replace(/[^A-Za-z0-9_-]/g, '');
  const [result, setResult] = useState<{ url: string; width?: number; alt: string } | 'invalid' | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    let url: string | undefined;
    setResult(null);
    void renderDiagram(source, id, theme, controller.signal).then(image => {
      if (!image || controller.signal.aborted) return;
      // An SVG image stays out of the chat's DOM and has no active content.
      url = URL.createObjectURL(new Blob([image.svg], { type: 'image/svg+xml' }));
      setResult({ url, width: image.width, alt: image.alt });
    }).catch(() => { if (!controller.signal.aborted) setResult('invalid'); });
    return () => {
      controller.abort();
      if (url) URL.revokeObjectURL(url);
    };
  }, [source, id, theme]);

  if (!result || result === 'invalid') return <>{result === 'invalid' && <p className="agent-diagram-error" role="status">Could not render Mermaid diagram.</p>}{fallback}</>;
  return <div className="agent-mermaid-scroll" role="region" aria-label="Mermaid diagram" tabIndex={0}>
    <img src={result.url} width={result.width} alt={result.alt} onError={() => setResult('invalid')} />
  </div>;
}

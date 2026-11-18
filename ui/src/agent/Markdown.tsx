import { createContext, memo, useContext } from 'react';
import ReactMarkdown, { type Components, type UrlTransform } from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { safeUrl } from './media.js';
import { MarkdownCodeBlock } from './MarkdownCodeBlock.js';

const streamingResponse = createContext(false);
const plugins = [remarkGfm];
const transformUrl: UrlTransform = (url, key) => key === 'href' && url.startsWith('#') ? url : safeUrl(url, key === 'src');
const components: Components = {
  pre: function CodeBlock({ node, children }) {
    const streaming = useContext(streamingResponse);
    const code = node?.children[0];
    if (code?.type !== 'element' || code.tagName !== 'code') return <pre>{children}</pre>;
    const source = code.children.map(child => child.type === 'text' ? child.value : '').join('').replace(/\n$/, '');
    const classes = code.properties.className;
    const mermaid = Array.isArray(classes) && classes.some(value => typeof value === 'string' && value.toLowerCase() === 'language-mermaid');
    return <MarkdownCodeBlock source={source} diagram={mermaid && !streaming && !!source.trim()}>{children}</MarkdownCodeBlock>;
  },
  a: ({ href, children }) => href ? <a href={href} rel="noopener noreferrer" target={href.startsWith('#') ? undefined : '_blank'}>{children}</a> : <span>{children}</span>,
  img: ({ src, alt }) => src ? <img className="agent-image" src={src} alt={alt ?? ''} loading="lazy" /> : <span>{alt}</span>,
  table: ({ children }) => <div className="agent-markdown-table" role="region" aria-label="Table" tabIndex={0}><table>{children}</table></div>,
};

export const Markdown = memo(function Markdown({ text, streaming = false }: { text: string; streaming?: boolean }) {
  return <div className="agent-markdown"><streamingResponse.Provider value={streaming}><ReactMarkdown remarkPlugins={plugins} components={components} urlTransform={transformUrl} skipHtml>{text}</ReactMarkdown></streamingResponse.Provider></div>;
});

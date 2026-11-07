import { memo } from 'react';
import ReactMarkdown, { type Components, type UrlTransform } from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { safeUrl } from './media.js';

const plugins = [remarkGfm];
const transformUrl: UrlTransform = (url, key) => key === 'href' && url.startsWith('#') ? url : safeUrl(url, key === 'src');
const components: Components = {
  a: ({ href, children }) => href ? <a href={href} rel="noopener noreferrer" target={href.startsWith('#') ? undefined : '_blank'}>{children}</a> : <span>{children}</span>,
  img: ({ src, alt }) => src ? <img className="agent-image" src={src} alt={alt ?? ''} loading="lazy" /> : <span>{alt}</span>,
  table: ({ children }) => <div className="agent-markdown-table" role="region" aria-label="Table" tabIndex={0}><table>{children}</table></div>,
};

export const Markdown = memo(function Markdown({ text }: { text: string }) {
  return <div className="agent-markdown"><ReactMarkdown remarkPlugins={plugins} components={components} urlTransform={transformUrl} skipHtml>{text}</ReactMarkdown></div>;
});

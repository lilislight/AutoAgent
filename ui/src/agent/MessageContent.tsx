import { mediaUrl } from './media.js';
import type { Content } from './types.js';
import { Markdown } from './Markdown.js';
import { config } from '../http.js';
export { Markdown } from './Markdown.js';

export function MessageContent({ content }: { content: Content[] }) {
  return <>{content.map((part, index) => {
    if (part.type === 'text' || part.type === 'refusal') return <Markdown key={index} text={part.text ?? ''} />;
    const source = part.source;
    const imageId = part.type === 'image' && source?.type === 'asset' && typeof source.uri === 'string'
      ? /^asset:\/\/agent-app-images\/([0-9a-f]{64})$/.exec(source.uri)?.[1] : undefined;
    const url = imageId ? `${config.apiPrefix}/agent/images/${imageId}` : mediaUrl(source, part.type);
    if (part.type === 'image' && url) return <img className="agent-image" key={index} src={url} alt="Agent attachment" loading="lazy" />;
    if (part.type === 'audio' && url) return <audio key={index} controls src={url} preload="none" />;
    if (part.type === 'video' && url) return <video key={index} controls src={url} preload="none" />;
    return <div className="agent-attachment" key={index}>{url ? <a href={url} target="_blank" rel="noopener noreferrer">Open {part.type}</a> : `${part.type} attachment · preview unavailable`}</div>;
  })}</>;
}

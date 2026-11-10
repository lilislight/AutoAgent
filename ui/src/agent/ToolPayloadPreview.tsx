import { MessageContent } from './MessageContent.js';
import { previewToolResult, previewToolText, TOOL_PREVIEW_CHARACTERS, TOOL_PREVIEW_LINES, MAX_CONTENT_BLOCKS } from './toolPreview.js';
import type { Content, Json } from './types.js';

export function ToolPreviewNotice({ name, value }: { name: string; value: () => string }) {
  const download = () => {
    const url = URL.createObjectURL(new Blob([value()], { type: 'text/plain;charset=utf-8' }));
    const link = document.createElement('a'); link.href = url; link.download = name; link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  };
  return <div className="agent-tool-preview-notice"><span>Preview limited to {TOOL_PREVIEW_CHARACTERS.toLocaleString('en')} characters or {TOOL_PREVIEW_LINES} lines; up to {MAX_CONTENT_BLOCKS} content blocks.</span><button type="button" onClick={download}>Download full content</button></div>;
}

export function ToolArgumentsPreview({ arguments: raw, name }: { arguments: string; name: string }) {
  let text: string;
  try { text = JSON.stringify(JSON.parse(raw), null, 2); } catch { text = raw; }
  const preview = previewToolText(text);
  return <>{preview.truncated ? <ToolPreviewNotice name={`${name}-arguments.txt`} value={() => raw} /> : null}
    <div className="agent-tool-preview" tabIndex={0} role="region" aria-label="Tool arguments preview"><pre>{preview.text}</pre></div></>;
}

export function ToolResultPreview({ content, structured, name }: { content: Content[]; structured: Json; name: string }) {
  const preview = previewToolResult(content, structured);
  return <>{preview.truncated ? <ToolPreviewNotice name={`${name}-result.json`} value={() => JSON.stringify({ content, structured_content: structured }, null, 2)} /> : null}
    <div className="agent-tool-preview" tabIndex={0} role="region" aria-label="Tool result preview"><MessageContent content={preview.content} />{preview.structuredText ? <pre>{preview.structuredText}</pre> : null}</div></>;
}

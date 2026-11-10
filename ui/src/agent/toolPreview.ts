import type { Content, Json } from './types.js';

export const TOOL_PREVIEW_CHARACTERS = 50_000;
export const TOOL_PREVIEW_LINES = 500;
export const MAX_CONTENT_BLOCKS = 32;

/** One shared budget per payload, including split text and formatted JSON. */
class PreviewBudget {
  private characters = TOOL_PREVIEW_CHARACTERS;
  private lines = TOOL_PREVIEW_LINES;
  truncated = false;

  take(text: string) {
    if (!this.characters || !this.lines) { this.truncated ||= text.length > 0; return ''; }
    let end = Math.min(text.length, this.characters);
    // Keep a UTF-16 surrogate pair together at the preview boundary.
    if (end > 0 && end < text.length && /[\uD800-\uDBFF]/.test(text[end - 1])) end--;
    let lines = 0;
    for (let index = 0; index < end; index++) {
      if (text[index] === '\n' && ++lines === this.lines) { end = index + 1; break; }
    }
    const value = text.slice(0, end);
    this.characters -= end;
    // Separate content blocks occupy separate rendered lines as well.
    this.lines -= value ? Math.max(1, lines + (value.endsWith('\n') ? 0 : 1)) : 0;
    this.truncated ||= end < text.length;
    return value;
  }
}

export function previewToolText(text: string) {
  const budget = new PreviewBudget();
  return { text: budget.take(text), truncated: budget.truncated };
}

export function previewToolStrings(texts: string[]) {
  const budget = new PreviewBudget();
  return { texts: texts.map(text => budget.take(text)), truncated: budget.truncated };
}

export function previewToolResult(content: Content[], structured: Json) {
  const budget = new PreviewBudget();
  const selected: Content[] = [];
  for (const part of content.slice(0, MAX_CONTENT_BLOCKS)) {
    if (part.type === 'text' || part.type === 'refusal') {
      const text = budget.take(part.text ?? '');
      if (text) selected.push({ ...part, text });
    } else selected.push(part);
  }
  const structuredText = structured === null ? null : budget.take(JSON.stringify(structured, null, 2));
  return { content: selected, structuredText, truncated: budget.truncated || content.length > MAX_CONTENT_BLOCKS };
}

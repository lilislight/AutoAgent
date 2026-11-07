import { jsonExample } from './jsonSchema.js';
import type { ExecutionActionSpec } from './types.js';

export interface ResponseDraft { text?: string; requestKey?: string }

export function responseEditor(spec: ExecutionActionSpec | undefined, draft: ResponseDraft) {
  const requestKey = JSON.stringify(spec?.request ?? null);
  return {
    requestKey,
    text: draft.text ?? JSON.stringify(spec?.response_example ?? jsonExample(spec?.input_schema), null, 2),
    stale: spec !== undefined && draft.text !== undefined && draft.requestKey !== requestKey,
  };
}

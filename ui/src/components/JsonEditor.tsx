import { lazy, Suspense } from 'react';
import type { FieldError } from '../jsonSchema';
const Editor = lazy(() => import('./JsonEditorImpl').then(module => ({ default: module.JsonEditor })));
export function JsonEditor(props: { value: string; onChange: (value: string) => void; schema?: unknown; label: string; disabled?: boolean; errors?: FieldError[] }) {
  return <Suspense fallback={<p className="json-editor-loading" role="status">Loading JSON editor…</p>}><Editor {...props} /></Suspense>;
}

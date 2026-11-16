import { CodeSkeleton } from './LoadingState';
import { lazy, Suspense } from 'react';
import type { FieldError } from '../jsonSchema';
const Editor = lazy(() => import('./JsonEditorImpl').then(module => ({ default: module.JsonEditor })));
export interface JsonEditorProps {
  value: string;
  onChange: (value: string) => void;
  schema?: unknown;
  label: string;
  disabled?: boolean;
  errors?: FieldError[];
  language?: 'json' | 'yaml';
  onFormat?: () => void;
  example?: boolean;
}
export function JsonEditor(props: JsonEditorProps) {
  return <Suspense fallback={<div className="json-editor-loading"><label>{props.label}</label><CodeSkeleton label={`Loading ${props.label} editor`} lines={8} /></div>}><Editor {...props} /></Suspense>;
}

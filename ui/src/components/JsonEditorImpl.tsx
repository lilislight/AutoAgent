import { useEffect, useRef, useState } from 'react';
import { Compartment, EditorState } from '@codemirror/state';
import { EditorView, keymap, lineNumbers, highlightActiveLine, hoverTooltip } from '@codemirror/view';
import { defaultKeymap, history, historyKeymap, indentWithTab } from '@codemirror/commands';
import { autocompletion, completionKeymap } from '@codemirror/autocomplete';
import { json } from '@codemirror/lang-json';
import { yaml, yamlLanguage } from '@codemirror/lang-yaml';
import { HighlightStyle, syntaxHighlighting } from '@codemirror/language';
import { tags } from '@lezer/highlight';
import { forceLinting, linter, lintGutter } from '@codemirror/lint';
import { jsonCompletion, jsonExample, jsonHover, jsonIssues, type FieldError, type JsonIssue } from '../jsonSchema';
import type { JsonEditorProps } from './JsonEditor';

const NO_ERRORS: FieldError[] = [];
const jsonHighlight = HighlightStyle.define([
  { tag: tags.propertyName, class: 'json-token-key' },
  { tag: tags.string, class: 'json-token-string' },
  { tag: tags.number, class: 'json-token-number' },
  { tag: [tags.bool, tags.null], class: 'json-token-literal' },
  { tag: tags.punctuation, class: 'json-token-punctuation' },
  { tag: tags.escape, class: 'json-token-escape' },
  { tag: tags.comment, class: 'json-token-comment' },
  { tag: tags.invalid, class: 'json-token-invalid' },
]);

function editorIssues(value: string, language: NonNullable<JsonEditorProps['language']>, schema: unknown, errors: FieldError[]) {
  if (language === 'json') return jsonIssues(value, schema, errors);
  const issues: JsonIssue[] = [];
  yamlLanguage.parser.parse(value).iterate({ enter(node) {
    if (node.type.isError && issues.length < 30) issues.push({
      from: node.from, to: Math.min(value.length, Math.max(node.to, node.from + 1)),
      path: [], code: 'syntax', message: 'Invalid YAML syntax.',
    });
  } });
  return issues;
}

export function JsonEditor({ value, onChange, schema, label, disabled = false, errors = NO_ERRORS, language = 'json', onFormat, example = true }: JsonEditorProps) {
  const parent = useRef<HTMLDivElement>(null), editor = useRef<EditorView | null>(null);
  const current = useRef({ onChange, schema, errors }); current.current = { onChange, schema, errors };
  const editable = useRef(new Compartment()), syncing = useRef(false);
  const [issues, setIssues] = useState<JsonIssue[]>([]);
  useEffect(() => { const timer = setTimeout(() => setIssues(editorIssues(value, language, schema, errors)), 350); return () => clearTimeout(timer); }, [value, schema, errors, language]);
  useEffect(() => {
    if (!parent.current) return;
    const view = new EditorView({ parent: parent.current, state: EditorState.create({ doc: value, extensions: [
      language === 'yaml' ? yaml() : json(), syntaxHighlighting(jsonHighlight), lineNumbers(), history(), highlightActiveLine(), lintGutter(),
      EditorView.lineWrapping, EditorState.tabSize.of(2), editable.current.of(EditorState.readOnly.of(disabled)),
      EditorView.contentAttributes.of({ 'aria-label': label, 'aria-multiline': 'true', role: 'textbox', spellcheck: 'false' }),
      keymap.of([...completionKeymap, ...defaultKeymap, ...historyKeymap, indentWithTab]),
      EditorView.updateListener.of(update => { if (update.docChanged && !syncing.current) current.current.onChange(update.state.doc.toString()); }),
      linter(view => editorIssues(view.state.doc.toString(), language, current.current.schema, current.current.errors).map(issue => ({ from: issue.from, to: issue.to, severity: 'error', message: `${issue.path.length ? issue.path.join('.') + ': ' : ''}${issue.message}` })), { delay: 350 }),
      ...(language === 'json' ? [autocompletion({ override: [context => {
        const text = context.state.doc.toString(), info = jsonCompletion(text, context.pos, current.current.schema);
        const word = context.matchBefore(/[\w$-]*/);
        if (!context.explicit && !word?.text && !info.location.isAtPropertyKey) return null;
        const quoted = context.state.sliceDoc(Math.max(0, (word?.from ?? context.pos) - 1), word?.from ?? context.pos) === '"';
        const from = quoted ? (word?.from ?? context.pos) - 1 : word?.from ?? context.pos;
        const closing = text.indexOf('"', context.pos);
        const to = quoted && closing >= context.pos && closing <= (word?.to ?? context.pos) + 1 ? closing + 1 : context.pos;
        const hasColon = /^\s*:/.test(text.slice(to));
        const options = info.location.isAtPropertyKey ? info.properties.map(item => ({ label: item.key, type: 'property', detail: `${item.schema.type ?? ''}${item.required ? ' · required' : ''}`, info: item.schema.description, apply: hasColon ? JSON.stringify(item.key) : `${JSON.stringify(item.key)}: ${JSON.stringify(jsonExample(current.current.schema, item.schema))}` })) :
          (info.target.enum ?? ('const' in info.target ? [info.target.const] : info.target.type === 'boolean' ? [true, false] : [])).map((item: unknown) => ({ label: JSON.stringify(item), type: 'value', apply: JSON.stringify(item) }));
        return options.length ? { from, to, options } : null;
      }] }),
      hoverTooltip((view, position) => {
        const info = jsonHover(view.state.doc.toString(), position, current.current.schema);
        if (!info || !Object.keys(info.schema).length) return null;
        return { pos: info.from, end: info.to, create() { const dom = document.createElement('div'); dom.className = 'json-schema-tooltip';
          dom.textContent = [info.path.join('.'), info.schema.description, info.schema.type ? `Type: ${info.schema.type}` : '', info.schema.enum ? `Options: ${info.schema.enum.join(', ')}` : ''].filter(Boolean).join('\n'); return { dom }; } };
      })] : []),
    ] }) });
    editor.current = view;
    return () => { editor.current = null; view.destroy(); };
    // Schema and callbacks are read through a ref to preserve cursor and undo state.
  }, [language, label]);
  useEffect(() => {
    const view = editor.current;
    if (!view || view.state.doc.toString() === value) return;
    syncing.current = true;
    try { view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: value } }); }
    finally { syncing.current = false; }
  }, [value]);
  useEffect(() => { editor.current?.dispatch({ effects: editable.current.reconfigure(EditorState.readOnly.of(disabled)) }); }, [disabled]);
  useEffect(() => { if (editor.current) forceLinting(editor.current); }, [schema, errors]);
  const format = onFormat ?? (() => {
    try { onChange(JSON.stringify(JSON.parse(value), null, 2)); } catch { editor.current?.focus(); }
  });
  return <div className={`json-editor ${disabled ? 'is-disabled' : ''}`}><div className="json-editor-tools"><span>{label}</span><button type="button" disabled={disabled} onClick={format}>Format</button>{example && <button type="button" disabled={disabled || schema === undefined} onClick={() => onChange(JSON.stringify(jsonExample(schema), null, 2))}>Reset example</button>}</div><div ref={parent} />
    {issues.length ? <ul className="json-editor-errors" aria-label={`${label} errors`}>{issues.map((issue, index) => <li key={index}><button type="button" onClick={() => { const view = editor.current; if (!view) return; view.dispatch({ selection: { anchor: issue.from, head: issue.to }, effects: EditorView.scrollIntoView(issue.from, { y: 'center' }) }); view.focus(); }}><code>{issue.path.length ? issue.path.join('.') : `Line ${value.slice(0, issue.from).split('\n').length}`}</code> {issue.message}</button></li>)}</ul> : language === 'json' ? <small className="json-editor-hint">JSON · Ctrl+Space for suggestions</small> : null}
  </div>;
}

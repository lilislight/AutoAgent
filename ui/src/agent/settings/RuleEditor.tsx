import { useEffect, useId, useMemo, useRef, useState } from 'react';
import { BusyLabel } from '../../components/LoadingState';
import { AgentIcon } from '../AgentIcon';
import { newId } from '../ids';
import { settingsApi, type PolicyPreview } from './api';
import { DecisionSelect, FormActions, FormError, errorText } from './Controls';
import type { Operator, Policy, Rule } from './types';
import { buildCondition, draftCondition, fieldOperators, newCondition, schemaFields, schemaPaths, type DraftCondition } from './policyState';

interface DraftRule extends Omit<Rule, 'when'> { key: string; when: DraftCondition }
const draftRules = (rules: Rule[]): DraftRule[] => rules.map(rule => ({ ...rule, key: newId(), when: draftCondition(rule.when) }));

function ConditionEditor({ value, onChange, paths, fields, relative = false, depth = 0, label }: { value: DraftCondition; onChange: (value: DraftCondition) => void; paths: string; fields: Record<string, Record<string, unknown>>; relative?: boolean; depth?: number; label: string }) {
  const patch = (update: Partial<DraftCondition>) => onChange({ ...value, ...update });
  const child = (index: number, next: DraftCondition) => patch({ children: value.children.map((item, position) => position === index ? next : item) });
  const items = value.kind === 'field' && (value.op === 'any_item' || value.op === 'all_items');
  const choices = fieldOperators(relative ? undefined : fields[value.field]);
  return <div className="settings-expression">
    <div className="settings-expression-heading"><label>Condition<select aria-label={`${label} type`} value={value.kind} onChange={event => {
      const kind = event.target.value as DraftCondition['kind'];
      if (kind === 'field') patch({ kind });
      else patch({ kind, children: value.kind === 'field' ? [{ ...value }] : value.children.slice(0, kind === 'not' ? 1 : undefined) });
    }}><option value="field">Field</option><option value="all">All of</option><option value="any">Any of</option><option value="not">Not</option></select></label></div>
    {value.kind === 'field' ? <div className="settings-condition-fields">
      <label>Field<input aria-label={`${label} field`} list={relative ? undefined : paths} value={value.field} onChange={event => patch({ field: event.target.value })} placeholder={relative ? '$ or item field' : 'args.path'} /></label>
      <label>Operator<select aria-label={`${label} operator`} value={value.op} onChange={event => {
        const op = event.target.value as Operator;
        patch({ op, children: value.children.length ? value.children : [newCondition('$')] });
      }}>{[...new Set([...choices, value.op])].map(op => <option value={op} key={op}>{op}</option>)}</select></label>
      {!items ? <label>Value · JSON or text{value.op === 'exists' ? <select aria-label={`${label} value`} value={value.valueText} onChange={event => patch({ valueText: event.target.value })}><option value="true">true</option><option value="false">false</option>{!['true', 'false'].includes(value.valueText) ? <option value={value.valueText}>Choose a value</option> : null}</select> : <input aria-label={`${label} value`} value={value.valueText} onChange={event => patch({ valueText: event.target.value })} spellCheck={false} />}</label> : null}
      {value.op === 'regex' ? <label>Flags<input aria-label={`${label} regex flags`} value={value.flags} onChange={event => patch({ flags: event.target.value })} placeholder="ims" title="i: ignore case; m: multiline; s: dot matches newlines" /></label> : null}
    </div> : null}
    {value.kind !== 'field' || items ? <div className="settings-expression-children">{value.children.map((item, index) => <div className="settings-expression-child" key={index}>
      <ConditionEditor value={item} onChange={next => child(index, next)} paths={paths} fields={fields} relative={relative || items} depth={depth + 1} label={`${label} condition ${index + 1}`} />
      {(value.kind === 'all' || value.kind === 'any') && value.children.length > 1 ? <button className="settings-icon" type="button" aria-label={`Remove ${label} condition ${index + 1}`} onClick={() => patch({ children: value.children.filter((_, i) => i !== index) })}><AgentIcon name="close" /></button> : null}
    </div>)}{(value.kind === 'all' || value.kind === 'any') && depth < 30 ? <button type="button" onClick={() => patch({ children: [...value.children, newCondition(relative ? '$' : 'args.path')] })}>Add condition</button> : null}</div> : null}
  </div>;
}

export function RuleEditor({ name, initial, schema, operationSchema, onSave, onCancel }: { name: string; initial: Policy; schema?: Record<string, unknown>; operationSchema?: Record<string, unknown> | null; onSave: (policy: Policy) => Promise<void>; onCancel: () => void }) {
  const [fallback, setFallback] = useState(initial.default), [rules, setRules] = useState(() => draftRules(initial.rules));
  const [mode, setMode] = useState<'form' | 'json' | 'yaml'>('form'), [text, setText] = useState('');
  const [args, setArgs] = useState('{}'), [operations, setOperations] = useState('[]'), [context, setContext] = useState('{}');
  const [toolName, setToolName] = useState(name), [busy, setBusy] = useState(false), [switchingTo, setSwitchingTo] = useState<typeof mode | null>(null), [previewBusy, setPreviewBusy] = useState(false);
  const [error, setError] = useState<string | null>(null), [preview, setPreview] = useState<PolicyPreview | null>(null);
  const request = useRef<AbortController | null>(null), id = useId();
  const fields = useMemo(() => ({ ...schemaFields(schema), ...schemaFields(operationSchema ?? undefined, 'operation'),
    'context.user_id': { type: 'string' }, 'context.session_id': { type: 'string' }, 'context.tool_name': { type: 'string' },
    'context.call_id': { type: 'string' }, 'context.attributes': { type: 'object' } }), [schema, operationSchema]);
  useEffect(() => () => request.current?.abort(), []);
  const signal = () => { request.current?.abort(); request.current = new AbortController(); return request.current.signal; };
  const formPolicy = (): Policy => ({ default: fallback, rules: rules.map(({ key: _key, when, ...rule }) => ({ ...rule, when: buildCondition(when) })) });
  const normalize = async () => {
    const result = await settingsApi.parsePolicy(mode === 'form' ? JSON.stringify(formPolicy()) : text, mode === 'yaml' ? 'yaml' : 'json', signal());
    setFallback(result.policy.default);
    return result;
  };
  const updateRule = (index: number, patch: Partial<DraftRule>) => { setRules(values => values.map((rule, i) => i === index ? { ...rule, ...patch } : rule)); setPreview(null); };
  const changeMode = async (next: typeof mode) => {
    if (next === mode) return;
    setSwitchingTo(next); setError(null);
    try { const normalized = await normalize(); setFallback(normalized.policy.default); setRules(draftRules(normalized.policy.rules)); setText(next === 'yaml' ? normalized.yaml : JSON.stringify(normalized.policy, null, 2)); setMode(next); }
    catch (error) { if (!request.current?.signal.aborted) setError(errorText(error)); }
    finally { setSwitchingTo(null); }
  };
  const test = async () => {
    setPreviewBusy(true); setError(null); setPreview(null);
    try { const normalized = await normalize(); const result = await settingsApi.preview(normalized.policy, toolName, JSON.parse(args), operationSchema ? JSON.parse(operations) : [], JSON.parse(context), signal()); setPreview(result); }
    catch (error) { if (!request.current?.signal.aborted) setError(errorText(error)); }
    finally { setPreviewBusy(false); }
  };
  const switching = switchingTo !== null;
  const locked = busy || previewBusy || switching;
  return <form className="settings-form settings-policy-form" aria-busy={locked} onSubmit={async event => {
    event.preventDefault(); if (locked) return; setBusy(true); setError(null);
    try { const normalized = await normalize(); await onSave(normalized.policy); }
    catch (error) { if (!request.current?.signal.aborted) setError(errorText(error)); }
    finally { setBusy(false); }
  }}>
    <fieldset disabled={locked}><div className="settings-subheading"><label>Default decision<DecisionSelect label="Default decision" value={fallback} disabled={mode !== 'form'} onChange={value => { setFallback(value); setPreview(null); }} /></label><div className="settings-policy-modes" role="group" aria-label="Policy format">{(['form', 'json', 'yaml'] as const).map(value => <button type="button" key={value} aria-pressed={mode === value} onClick={() => void changeMode(value)}><BusyLabel busy={switchingTo === value} label={value === 'form' ? 'Builder' : value.toUpperCase()}>{value === 'form' ? 'Builder' : value.toUpperCase()}</BusyLabel></button>)}</div></div>
    <span className="settings-origin">Deny &gt; Ask &gt; Allow</span>
    {mode !== 'form' ? <label>Policy {mode.toUpperCase()}<textarea className="settings-code-input" rows={14} value={text} onChange={event => { setText(event.target.value); setPreview(null); }} spellCheck={false} /></label> : <>
      <datalist id={`${id}-paths`}>{Object.keys(fields).map(path => <option value={path} key={path} />)}</datalist>
      {rules.map((rule, index) => <section className="settings-policy-rule" key={rule.key}><div className="settings-rule-heading"><strong>Rule {index + 1}</strong><DecisionSelect label={`Rule ${index + 1} decision`} value={rule.decision} onChange={decision => updateRule(index, { decision })} /><button className="settings-icon" type="button" aria-label={`Delete rule ${index + 1}`} onClick={() => { setRules(values => values.filter((_, i) => i !== index)); setPreview(null); }}><AgentIcon name="close" /></button></div>
        <ConditionEditor value={rule.when} onChange={when => updateRule(index, { when })} paths={`${id}-paths`} fields={fields} label={`Rule ${index + 1}`} />
        <div className="settings-rule-bottom"><label>Rule ID<input value={rule.id ?? ''} onChange={event => updateRule(index, { id: event.target.value || null })} /></label><label>Reason<input value={rule.reason ?? ''} onChange={event => updateRule(index, { reason: event.target.value || null })} /></label></div>
      </section>)}<button type="button" disabled={rules.length >= 256} onClick={() => { setRules(values => [...values, { key: newId(), id: newId(), decision: 'ask', when: newCondition(schemaPaths(schema)[0] ?? 'args') }]); setPreview(null); }}>Add rule</button>
    </>}
    {schema ? <details className="settings-input-schema"><summary>Tool input schema</summary><pre>{JSON.stringify(schema, null, 2)}</pre></details> : null}
    <section className="settings-policy-preview"><h4>Try an example</h4><label>Tool name<input value={toolName} onChange={event => { setToolName(event.target.value); setPreview(null); }} /></label><label>Arguments JSON<textarea rows={3} value={args} onChange={event => { setArgs(event.target.value); setPreview(null); }} spellCheck={false} /></label>
      {operationSchema ? <details><summary>Operations JSON</summary><textarea aria-label="Operations JSON" rows={4} value={operations} onChange={event => { setOperations(event.target.value); setPreview(null); }} spellCheck={false} /></details> : null}
      <details><summary>Context JSON</summary><textarea aria-label="Context JSON" rows={3} value={context} onChange={event => { setContext(event.target.value); setPreview(null); }} spellCheck={false} placeholder={'{"user_id":"alice","attributes":{"role":"reviewer"}}'} /></details>
      <div className="settings-connection-test"><button type="button" onClick={() => void test()}><BusyLabel busy={previewBusy} label="Checking">Preview decision</BusyLabel></button>{preview ? <output className={`settings-policy-result ${preview.decision.action}`}>{preview.decision.action}{preview.decision.reason ? ` · ${preview.decision.reason}` : ''}</output> : null}</div>
      {preview ? <ul className="settings-policy-matches">{preview.operations.map((operation, index) => <li key={index}>{operation.default_used ? 'Default decision' : operation.matched_rules.map(rule => rule.id ?? `Rule ${rule.index + 1}`).join(', ')}</li>)}</ul> : null}
    </section></fieldset><FormError message={error} /><FormActions busy={busy} disabled={previewBusy || switching} onCancel={onCancel} />
  </form>;
}

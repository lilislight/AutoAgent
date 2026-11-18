import { useEffect, useId, useMemo, useRef, useState, type ReactNode } from 'react';
import { JsonEditor } from '../../components/JsonEditor';
import { jsonExample, jsonIssues } from '../../jsonSchema';
import { BusyLabel } from '../../components/LoadingState';
import { AgentIcon } from '../AgentIcon';
import { newId } from '../../ids';
import { settingsApi, type PolicyPreview } from './api';
import { DecisionSelect, FormActions, FormError, errorText } from './Controls';
import type { Decision, Operator, Policy, Resource, Rule } from './types';
import { buildCondition, conditionSummary, draftCondition, fieldOperators, newCondition, retainRuleMetadata, schemaFields, type DraftCondition } from './policyState';

interface DraftRule { key: string; decision: Decision; when: DraftCondition }
const draftRules = (rules: Rule[]): DraftRule[] => rules.map(({ when, decision }) => ({ decision, key: newId(), when: draftCondition(when) }));
type Format = 'form' | 'json' | 'yaml';
const textFormats = ['json', 'yaml'] as const;
const draftContent = ({ when, decision }: DraftRule) => ({ when, decision });

const contextSchema = {
  type: 'object', additionalProperties: false,
  examples: [{ user_id: 'example-user', session_id: 'example-session', attributes: {} }],
  properties: {
    user_id: { type: ['string', 'null'] },
    session_id: { type: ['string', 'null'] },
    attributes: { type: 'object' },
  },
};
const sourceLabels = { code: 'Code', group: 'Tool group', source: 'MCP server', file_read: 'File read', custom: 'Custom' };
const decisionLabel = (action: Decision) => action[0].toUpperCase() + action.slice(1);

function RuleRow({ label, decision, source, expanded, onToggle, children }: {
  label: string;
  decision: Decision | 'runtime';
  source: keyof typeof sourceLabels;
  expanded?: boolean;
  onToggle?: () => void;
  children?: ReactNode;
}) {
  const panelId = useId();
  const cells = <>
    <span className="settings-rule-condition">
      {onToggle ? <span className={`settings-chevron${expanded ? ' open' : ''}`}><AgentIcon name="chevron" /></span> : <span />}
      <span title={label}>{label}</span>
    </span>
    {decision === 'runtime' ? <span>Runtime</span> :
      <span className={`settings-decision-tag ${decision}`}>{decisionLabel(decision)}</span>}
    <span className="settings-rule-source">{sourceLabels[source]}{source !== 'custom' ? <><AgentIcon name="access" /><span className="sr-only">, read only</span></> : null}</span>
  </>;
  return <li className={`settings-rule-item${expanded ? ' is-open' : ''}`}>
    {onToggle ? <button type="button" className="settings-rule-row" aria-expanded={expanded} aria-controls={panelId} onClick={onToggle}>{cells}</button>
      : <div className="settings-rule-row">{cells}</div>}
    {onToggle ? <div id={panelId} className="settings-rule-details" hidden={!expanded}>{expanded ? children : null}</div> : null}
  </li>;
}

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

function TestResult({ preview }: { preview: PolicyPreview }) {
  return <section className="settings-test-result" aria-label="Permission test result">
    <ul>{preview.operations.map(({ operation, decision, defaults, matched_rules }, index) => <li key={index}>
      {operation ? <div className="settings-test-target"><code>{operation.action}</code>{operation.resource && <span>{operation.resource}</span>}<strong>{decisionLabel(decision.action)}</strong></div> : null}
      <div className="settings-test-matches">
        {matched_rules.map(rule => <span key={`${rule.layer}:${rule.index}`}>{sourceLabels[rule.source]} rule {rule.index + 1} <strong>{decisionLabel(rule.decision)}</strong></span>)}
        {defaults.filter(rule => rule.decision !== 'allow' || !matched_rules.length).map(rule =>
          <span key={rule.layer}>{sourceLabels[rule.source]} default <strong>{decisionLabel(rule.decision)}</strong></span>)}
        {!matched_rules.length && !defaults.length ? <span>{decision.reason ?? 'No rule evaluated'}</span> : null}
      </div>
    </li>)}</ul>
  </section>;
}

interface RuleEditorProps {
  resource: Resource;
  initial: Policy;
  policySchema?: Record<string, unknown>;
  workspace: string;
  onSave: (policy: Policy) => Promise<void>;
  onCancel: () => void;
  onAcceptSchema?: () => Promise<void>;
}

export function RuleEditor({ resource, initial, policySchema, workspace, onSave, onCancel, onAcceptSchema }: RuleEditorProps) {
  const { input_schema: schema, operation_schema: operationSchema } = resource;
  const [fallback, setFallback] = useState(initial.default);
  const [rules, setRules] = useState(() => draftRules(initial.rules));
  const [expandedRule, setExpandedRule] = useState<string | null>(null);
  const [mode, setMode] = useState<Format>('form');
  const [text, setText] = useState('');
  const [formattedText, setFormattedText] = useState('');
  const [testOpen, setTestOpen] = useState(false);
  const [args, setArgs] = useState(() => JSON.stringify(jsonExample(schema), null, 2));
  const [context, setContext] = useState('{}');
  const [busy, setBusy] = useState(false);
  const [switchingTo, setSwitchingTo] = useState<Format | null>(null);
  const [previewBusy, setPreviewBusy] = useState(false);
  const [reviewing, setReviewing] = useState(false);
  const [reviewError, setReviewError] = useState<string | null>(null);
  const [policyError, setPolicyError] = useState<string | null>(null);
  const [testError, setTestError] = useState<string | null>(null);
  const [preview, setPreview] = useState<PolicyPreview | null>(null);
  const request = useRef<AbortController | null>(null);
  const form = useRef<HTMLFormElement>(null);
  const focusRule = useRef(false);
  const options = useRef<HTMLDetailsElement>(null);
  const id = useId();
  const fields = useMemo(() => ({
    ...schemaFields(schema), ...schemaFields(operationSchema ?? undefined, 'operation'),
    'context.user_id': { type: 'string' }, 'context.session_id': { type: 'string' },
    'context.tool_name': { type: 'string' }, 'context.call_id': { type: 'string' }, 'context.attributes': { type: 'object' },
  }), [schema, operationSchema]);
  useEffect(() => () => request.current?.abort(), []);
  useEffect(() => {
    if (!focusRule.current) return;
    focusRule.current = false;
    const details = form.current?.querySelector('.settings-rule-item.is-open .settings-rule-details');
    (details?.querySelector('input') ?? details?.querySelector('select'))?.focus();
  }, [expandedRule, rules.length]);
  const originalRules = useMemo(() => JSON.stringify(initial.rules.map(({ decision, when }) => ({
    when: draftCondition(when), decision,
  }))), [initial]);
  const dirty = fallback !== initial.default || JSON.stringify(rules.map(draftContent)) !== originalRules
    || (mode !== 'form' && text !== formattedText);
  const toggleRule = (key: string) => setExpandedRule(current => current === key ? null : key);
  const invalidatePreview = () => { setPreview(null); setTestError(null); };
  const signal = () => {
    const controller = new AbortController();
    request.current?.abort();
    request.current = controller;
    return controller.signal;
  };
  const normalize = (requestSignal: AbortSignal) => {
    const form: Policy = { default: fallback, rules: [] };
    if (mode === 'form') form.rules = rules.map(({ when, decision }) => ({ when: buildCondition(when), decision }));
    return settingsApi.parsePolicy(mode === 'form' ? JSON.stringify(form) : text, mode === 'yaml' ? 'yaml' : 'json', requestSignal);
  };
  const updateRule = (index: number, patch: Partial<DraftRule>) => {
    setRules(values => values.map((rule, i) => i === index ? { ...rule, ...patch } : rule));
    setPolicyError(null);
    invalidatePreview();
  };
  const convert = async (next: Format) => {
    setSwitchingTo(next);
    setPolicyError(null);
    const requestSignal = signal();
    try {
      const normalized = await normalize(requestSignal);
      setFallback(normalized.policy.default);
      if (mode !== 'form') {
        setRules(draftRules(normalized.policy.rules));
        setExpandedRule(null);
      }
      const formatted = next === 'yaml' ? normalized.yaml : normalized.json;
      setText(formatted);
      setFormattedText(formatted);
      setMode(next);
    } catch (error) {
      if (!requestSignal.aborted) setPolicyError(errorText(error));
    } finally { setSwitchingTo(null); }
  };
  const test = async () => {
    invalidatePreview();
    setPolicyError(null);
    for (const [label, value, contract] of [['Arguments', args, schema], ['Context', context, contextSchema]] as const) {
      const issue = jsonIssues(value, contract)[0];
      if (issue) {
        setTestError(`${label}${issue.path.length ? ' · ' + issue.path.join('.') : ''} · ${issue.message}`);
        return;
      }
    }
    setPreviewBusy(true);
    const requestSignal = signal();
    try {
      let policy: Policy;
      try { ({ policy } = await normalize(requestSignal)); }
      catch (error) { if (!requestSignal.aborted) setPolicyError(errorText(error)); return; }
      const result = await settingsApi.preview(policy, resource.management_id, workspace, JSON.parse(args), JSON.parse(context), requestSignal);
      setPreview(result);
    } catch (error) {
      if (!requestSignal.aborted) setTestError(errorText(error));
    } finally { setPreviewBusy(false); }
  };
  const locked = busy || previewBusy || switchingTo !== null || reviewing;
  const canTest = !resource.mcp_status || resource.mcp_status === 'ready' || resource.mcp_status === 'review';
  const registered = resource.registered_policies ?? [];
  const hasRows = Boolean(resource.policy_in_code || rules.length || fallback !== 'allow' || expandedRule === 'fallback'
    || registered.some(({ policy }) => policy.rules.length || policy.default !== 'allow'));
  const addRule = () => {
    const rule = { key: newId(), decision: 'ask' as const, when: newCondition(Object.keys(schemaFields(schema))[0] ?? 'args') };
    focusRule.current = true;
    setRules(values => [...values, rule]);
    setExpandedRule(rule.key);
    setPolicyError(null);
    invalidatePreview();
  };
  const acceptSchema = async () => {
    setReviewing(true);
    setReviewError(null);
    try { await onAcceptSchema?.(); }
    catch (error) { setReviewError(errorText(error)); }
    finally { setReviewing(false); }
  };

  return <form ref={form} className="settings-form settings-policy-form" aria-busy={locked} onSubmit={async event => {
    event.preventDefault();
    if (locked || !dirty || !resource.permissions_editable) return;
    setBusy(true);
    setPolicyError(null);
    const requestSignal = signal();
    try {
      const { policy } = await normalize(requestSignal);
      if (!requestSignal.aborted) await onSave(retainRuleMetadata(policy, initial));
    }
    catch (error) { if (!requestSignal.aborted) setPolicyError(errorText(error)); }
    finally { setBusy(false); }
  }}>
    {onAcceptSchema ? <section className="settings-schema-review">
      <h4>Review changed schema</h4>
      <div className="settings-schema-columns">
        <details><summary>Previously accepted schema</summary><pre>{JSON.stringify(resource.approved_schema, null, 2)}</pre></details>
        <details open><summary>Current schema</summary><pre>{JSON.stringify(schema, null, 2)}</pre></details>
      </div>
      <p className="settings-help">The tool is paused until you accept the current schema.</p>
      <button type="button" disabled={locked} onClick={() => void acceptSchema()}>
        <BusyLabel busy={reviewing} label="Accepting">Accept schema</BusyLabel>
      </button>
      <FormError message={reviewError} />
    </section> : null}
    <fieldset disabled={locked} className="settings-rules">
      <div className="settings-policy-heading">
        <h4>Rules</h4>
        {resource.permissions_editable ? <div className="settings-policy-actions">
          {mode === 'form' ? <>
            <button type="button" disabled={rules.length >= 256} onClick={addRule}>Add rule</button>
            <details ref={options} className="settings-rule-options" onKeyDown={event => {
              if (event.key !== 'Escape' || !event.currentTarget.open) return;
              event.preventDefault();
              event.stopPropagation();
              event.currentTarget.open = false;
              event.currentTarget.querySelector('summary')?.focus();
            }}>
              <summary aria-label="Rule options">Options <AgentIcon name="chevron" /></summary>
              <div>
                <button type="button" disabled={locked} onClick={() => {
                  if (options.current) options.current.open = false;
                  if (expandedRule !== 'fallback') focusRule.current = true;
                  setExpandedRule('fallback');
                }}>Set custom fallback</button>
                {textFormats.map(format => <button type="button" key={format} disabled={locked} onClick={() => void convert(format)}>
                  <BusyLabel busy={switchingTo === format} label="Opening">Edit {format.toUpperCase()}</BusyLabel>
                </button>)}
              </div>
            </details>
          </> : <>
            <button type="button" onClick={() => void convert('form')}>Back to rules</button>
            <div className="settings-policy-modes" role="group" aria-label="Policy format">
              {textFormats.map(format => <button type="button" key={format} aria-pressed={mode === format} onClick={() => format !== mode && void convert(format)}>{format.toUpperCase()}</button>)}
            </div>
          </>}
        </div> : null}
      </div>
      {mode === 'form' ? <>
        <datalist id={`${id}-paths`}>{Object.keys(fields).map(path => <option value={path} key={path} />)}</datalist>
        {!hasRows ? <p className="settings-empty">No rules. Default: Allow.</p> : <>
          <div className="settings-rule-row settings-rules-columns" aria-hidden="true"><span>Condition</span><span>Decision</span><span>Source</span></div>
          <ul className="settings-rule-list" aria-label="Tool rules">
            {registered.flatMap(({ source, policy }, layer) => [
              ...policy.rules.map((rule, index) => {
                const key = `registered-${layer}-${index}`;
                return <RuleRow key={key} label={conditionSummary(draftCondition(rule.when))} decision={rule.decision}
                  source={source} expanded={expandedRule === key} onToggle={() => toggleRule(key)}>
                  <pre>{JSON.stringify(rule.when, null, 2)}</pre>
                </RuleRow>;
              }),
              ...(policy.default !== 'allow' ? [<RuleRow key={`default-${layer}`} label="Fallback for this policy" decision={policy.default} source={source} />] : []),
            ])}
            {resource.policy_in_code ? <RuleRow label="Code callback" source="code" decision="runtime"
              expanded={expandedRule === 'callback'} onToggle={() => toggleRule('callback')}>
              <p>The callback decides at runtime. The permission test evaluates declarative rules.</p>
            </RuleRow> : null}
            {rules.map((rule, index) => <RuleRow key={rule.key} label={conditionSummary(rule.when)} decision={rule.decision}
              source="custom" expanded={expandedRule === rule.key} onToggle={() => toggleRule(rule.key)}>
              {resource.permissions_editable ? <>
                <div className="settings-rule-heading">
                  <label>Decision<DecisionSelect label={`Rule ${index + 1} decision`} value={rule.decision} onChange={decision => updateRule(index, { decision })} /></label>
                  <button type="button" className="settings-danger" aria-label={`Delete rule ${index + 1}`} onClick={() => {
                    setRules(values => values.filter(value => value.key !== rule.key));
                    setExpandedRule(null); setPolicyError(null); invalidatePreview();
                  }}>Delete rule</button>
                </div>
                <ConditionEditor value={rule.when} onChange={when => updateRule(index, { when })} paths={`${id}-paths`} fields={fields} label={`Rule ${index + 1}`} />
              </> : <pre>{JSON.stringify(buildCondition(rule.when), null, 2)}</pre>}
            </RuleRow>)}
            {fallback !== 'allow' || expandedRule === 'fallback' ? <RuleRow label="Fallback for custom rules" decision={fallback}
              source="custom" expanded={expandedRule === 'fallback'} onToggle={resource.permissions_editable ? () => toggleRule('fallback') : undefined}>
              <label>Custom fallback<DecisionSelect label="Custom fallback" value={fallback} onChange={value => {
                setFallback(value); setPolicyError(null); invalidatePreview();
              }} /></label>
            </RuleRow> : null}
          </ul>
        </>}
      </> : <JsonEditor key={mode} label={`Policy ${mode.toUpperCase()}`} value={text} language={mode} schema={policySchema} example={false}
        disabled={locked} onFormat={() => void convert(mode)} onChange={value => { setText(value); setPolicyError(null); invalidatePreview(); }} />}
      <FormError message={policyError} />
    </fieldset>
    <details className="settings-policy-test" onToggle={event => setTestOpen(event.currentTarget.open)}>
      <summary>Test permission</summary>
      {testOpen ? <fieldset className="settings-policy-preview" disabled={locked || !canTest}>
        {!canTest ? <p className="settings-help">Connect the MCP server to test this tool.</p> : null}
        <JsonEditor label="Arguments JSON" value={args} schema={schema} disabled={locked || !canTest} onChange={value => { setArgs(value); invalidatePreview(); }} />
        <details><summary>Context JSON</summary>
          <JsonEditor label="Context JSON" value={context} schema={contextSchema} disabled={locked || !canTest} onChange={value => { setContext(value); invalidatePreview(); }} />
        </details>
        <div className="settings-test-actions">
          <button type="button" onClick={() => void test()}><BusyLabel busy={previewBusy} label="Checking">Check permission</BusyLabel></button>
          <output aria-live="polite">{preview ? <><span className="sr-only">Decision </span>
            <span className={`settings-decision-tag ${preview.decision.action}`}>{decisionLabel(preview.decision.action)}
              {!preview.includes_code_callbacks ? ' · Declarative rules' : ''}</span></> : null}</output>
        </div>
        <FormError message={testError} />
        {preview && <TestResult preview={preview} />}
        {preview && !preview.includes_code_callbacks ? <p className="settings-help">Code callbacks are evaluated at runtime.</p> : null}
      </fieldset> : null}
    </details>
    {resource.permissions_editable && dirty ? <FormActions busy={busy} disabled={locked} onCancel={onCancel} /> : null}
  </form>;
}

import type { Credential } from './types';

export interface ReferenceRow { key: string; source: Credential['source']; name: string }
export const referenceRows = (values: Record<string, Credential> = {}): ReferenceRow[] => Object.entries(values).map(([key, reference]) => ({ key, ...reference }));
export const referenceMap = (rows: ReferenceRow[]): Record<string, Credential> => {
  const values = rows.filter(row => row.key.trim() && row.name.trim());
  if (new Set(values.map(row => row.key.trim())).size !== values.length) throw new Error('Reference names must be unique.');
  return Object.fromEntries(values.map(row => [row.key.trim(), { source: row.source, name: row.name.trim() }]));
};
export function References({ rows, onChange, label }: { rows: ReferenceRow[]; onChange: (rows: ReferenceRow[]) => void; label: string }) {
  return <div className="settings-references"><div className="settings-subheading"><h4>{label}</h4><button type="button" onClick={() => onChange([...rows, { key: '', source: 'env', name: '' }])}>Add reference</button></div>{rows.map((row, index) => <div className="settings-reference-row" key={index}><label>Name<input aria-label={`${label} ${index + 1} name`} required value={row.key} onChange={event => onChange(rows.map((value, i) => i === index ? { ...value, key: event.target.value } : value))} placeholder={label === 'Headers' ? 'Authorization' : 'API_TOKEN'} /></label><label>Variable name<input aria-label={`${label} ${index + 1} variable`} required pattern="[A-Za-z_][A-Za-z0-9_]*" value={row.name} onChange={event => onChange(rows.map((value, i) => i === index ? { ...value, name: event.target.value } : value))} placeholder="MY_API_TOKEN" /></label><button type="button" className="settings-remove" aria-label={`Remove ${label} ${index + 1}`} onClick={() => onChange(rows.filter((_, i) => i !== index))}>Remove</button></div>)}</div>;
}

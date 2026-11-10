import { CopyId } from './InspectorFrame';

export function ActionReference({ label, value }: { label: string; value: string }) {
  return <div className="run-field action-reference"><span>{label}</span><CopyId label={label} value={value} /></div>;
}

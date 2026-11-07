import type { AgentExecution, SettingsRecord } from './types.js';

// A Child inherits its parent's execution settings before saving its own settings.
export function effectiveSettings(record: SettingsRecord | undefined, snapshot: AgentExecution['snapshot'] | undefined, child: boolean): SettingsRecord | undefined {
  return record && child && record.version === 0 && snapshot ? { ...record, settings: snapshot.settings } : record;
}

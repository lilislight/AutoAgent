import type { ObservationClock } from '../liveObservation';
import { createContext, useContext } from 'react';
import type { ExecutionRecordSelection, InspectorTab, TraceRow, PendingResponse, ExecutionActionKind, ExecutionActionInput, AcceptedExecution } from '../types';

export const ExecutionNavigationContext = createContext<{
  invocationId?: string; replaying?: boolean; paused?: boolean;
  onRunAccepted?: (receipt: AcceptedExecution) => void;
  sendAction?: (action: ExecutionActionKind, options: Partial<ExecutionActionInput>) => Promise<void>;
  actionBusy?: boolean; actionError?: string | null; actionErrors?: import('../jsonSchema').FieldError[]; clearActionError?: () => void;
  active: boolean; openChild?: (child: TraceRow) => void;
  pickRecord?: (kind: ExecutionRecordSelection['kind'], row: TraceRow, section?: InspectorTab) => void;
  showGraph?: () => void; showTimeline?: () => void;
  setSection?: (kind: string, section: InspectorTab) => void;
  exact?: boolean; inspectorKind?: string;
  replaySequence?: number; clock?: ObservationClock;
  respondToWait?: (row:TraceRow)=>void; canRespond?:boolean; pendingResponses?:PendingResponse[];
}>({ active: true });
export function useExecutionNavigation() { return useContext(ExecutionNavigationContext); }

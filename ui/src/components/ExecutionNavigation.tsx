import { createContext, useContext } from 'react';
import type { ExecutionRecordSelection, InspectorTab, TraceRow, PendingResponse } from '../types';

export const ExecutionNavigationContext = createContext<{
  active: boolean; openChild?: (child: TraceRow) => void;
  pickRecord?: (kind: ExecutionRecordSelection['kind'], row: TraceRow, section?: InspectorTab) => void;
  showGraph?: () => void; showTimeline?: () => void;
  setSection?: (kind: string, section: InspectorTab) => void;
  exact?: boolean; inspectorKind?: string;
  replaySequence?: number; nowUs?: number;
  respondToWait?: (row:TraceRow)=>void; canRespond?:boolean; pendingResponses?:PendingResponse[];
}>({ active: true });
export function useExecutionNavigation() { return useContext(ExecutionNavigationContext); }

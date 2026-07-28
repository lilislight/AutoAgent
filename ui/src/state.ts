import { create } from "zustand";

import type { TraceSelection } from "./types";

export type InspectorTab =
  | "overview"
  | "data"
  | "trace"
  | "user_events"
  | "context"
  | "definition";
interface TraceUiState {
  workflowId: string | null;
  sessionId: string | null;
  invocationId: string | null;
  cursorSequence: number | null;
  replayCursorSequence: number | null;
  followLive: boolean;
  selection: TraceSelection;
  inspectorTab: InspectorTab;
  setWorkflow: (id: string | null) => void;
  setSession: (id: string | null) => void;
  setInvocation: (id: string | null) => void;
  setInvocationScope: (
    workflowId: string,
    sessionId: string,
    invocationId: string,
  ) => void;
  setCursor: (sequence: number, followLive?: boolean) => void;
  setFollowLive: (value: boolean) => void;
  setSelection: (selection: TraceSelection) => void;
  setInspectorTab: (tab: InspectorTab) => void;
}

export const useTraceUi = create<TraceUiState>((set) => ({
  workflowId: null,
  sessionId: null,
  invocationId: null,
  cursorSequence: null,
  replayCursorSequence: null,
  followLive: true,
  selection: null,
  inspectorTab: "overview",
  setWorkflow: (workflowId) =>
    set({
      workflowId,
      sessionId: null,
      invocationId: null,
      cursorSequence: null,
      replayCursorSequence: null,
      followLive: true,
      selection: null,
    }),
  setSession: (sessionId) =>
    set({
      sessionId,
      invocationId: null,
      cursorSequence: null,
      replayCursorSequence: null,
      followLive: true,
      selection: null,
    }),
  setInvocation: (invocationId) =>
    set({
      invocationId,
      cursorSequence: null,
      replayCursorSequence: null,
      followLive: true,
      selection: null,
    }),
  setInvocationScope: (workflowId, sessionId, invocationId) =>
    set({
      workflowId,
      sessionId,
      invocationId,
      cursorSequence: null,
      replayCursorSequence: null,
      followLive: true,
      selection: null,
    }),
  setCursor: (cursorSequence, followLive = false) =>
    set((current) => ({
      cursorSequence,
      followLive,
      // A following update must not erase the user's last replay location.
      replayCursorSequence: followLive
        ? current.replayCursorSequence
        : cursorSequence,
    })),
  setFollowLive: (followLive) => set({ followLive }),
  setSelection: (selection) => set({ selection }),
  setInspectorTab: (inspectorTab) => set({ inspectorTab }),
}));

import { create } from "zustand";

import type { TraceSelection } from "./types";

interface TraceUiState {
  workflowId: string | null;
  sessionId: string | null;
  invocationId: string | null;
  cursorSequence: number | null;
  followLive: boolean;
  connected: boolean;
  selection: TraceSelection;
  setWorkflow: (id: string | null) => void;
  setSession: (id: string | null) => void;
  setInvocation: (id: string | null) => void;
  setCursor: (sequence: number, followLive?: boolean) => void;
  setFollowLive: (value: boolean) => void;
  setConnected: (value: boolean) => void;
  setSelection: (selection: TraceSelection) => void;
}

export const useTraceUi = create<TraceUiState>((set) => ({
  workflowId: null,
  sessionId: null,
  invocationId: null,
  cursorSequence: null,
  followLive: true,
  connected: false,
  selection: null,
  setWorkflow: (workflowId) =>
    set({
      workflowId,
      sessionId: null,
      invocationId: null,
      cursorSequence: null,
      followLive: true,
      selection: null,
    }),
  setSession: (sessionId) =>
    set({
      sessionId,
      invocationId: null,
      cursorSequence: null,
      followLive: true,
      selection: null,
    }),
  setInvocation: (invocationId) =>
    set({
      invocationId,
      cursorSequence: null,
      followLive: true,
      selection: null,
    }),
  setCursor: (cursorSequence, followLive = false) =>
    set({ cursorSequence, followLive }),
  setFollowLive: (followLive) => set({ followLive }),
  setConnected: (connected) => set({ connected }),
  setSelection: (selection) => set({ selection }),
}));

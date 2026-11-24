import { useEffect, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { config, request } from '../http.js';
import { refreshWorkflowCatalog } from './workflowCatalog.js';

export interface CatalogHead { version: string; workflows: Record<string, string> }

/** Reconcile on connect/visibility and retain content during silent refreshes. */
export function useWorkflowCatalogWatch(active: boolean, onChange: (previous: CatalogHead, current: CatalogHead) => void) {
  const client = useQueryClient(), previous = useRef<CatalogHead | null>(null), callback = useRef(onChange);
  callback.current = onChange;
  useEffect(() => {
    if (!active) return;
    let stopped = false, reading = false, source: EventSource | null = null, timer: ReturnType<typeof setTimeout> | undefined;
    const controller = new AbortController();
    const apply = (head: CatalogHead, reconcile = false) => {
      if (stopped || !head || typeof head.version !== 'string' || !head.workflows || typeof head.workflows !== 'object') return;
      const old = previous.current;
      if (reconcile || old?.version !== head.version) {
        void refreshWorkflowCatalog(client, () => !stopped).catch(() => {});
        if (old && old.version !== head.version) callback.current(old, head);
      }
      previous.current = head;
    };
    const poll = async () => {
      timer = undefined;
      if (stopped || reading) return;
      reading = true;
      try { apply(await request<CatalogHead>('/workflow-catalog/head?ui=workflow', controller.signal)); } catch { /* Keep the saved catalog; reconnect or the next read can reconcile. */ }
      finally { reading = false; }
      if (!stopped && !timer && (!source || source.readyState !== 1)) timer = setTimeout(poll, 3000);
    };
    if (typeof EventSource !== 'undefined') {
      source = new EventSource(`${config.apiPrefix}/workflow-catalog/watch?ui=workflow`);
      source.addEventListener('catalog', event => { try { apply(JSON.parse((event as MessageEvent<string>).data), true); } catch { void poll(); } });
      source.onerror = () => { if (!timer) timer = setTimeout(() => { timer = undefined; void poll(); }, 1000); };
    } else void poll();
    const visible = () => { if (!document.hidden) void request<CatalogHead>('/workflow-catalog/head?ui=workflow', controller.signal).then(head => apply(head, true)).catch(() => {}); };
    document.addEventListener('visibilitychange', visible);
    return () => { stopped = true; source?.close(); clearTimeout(timer); controller.abort(); document.removeEventListener('visibilitychange', visible); };
  }, [active, client]);
}

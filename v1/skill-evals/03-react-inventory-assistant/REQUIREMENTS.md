# Inventory Replenishment Assistant

Build an AI assistant that recommends an inventory replenishment plan for a
warehouse.

## Request and result

The request contains a natural-language question and a `warehouse_id`.

Return:

- `warehouse_id`
- `sku`
- `recommended_quantity`
- `supplier`
- `reasoning_summary`
- `tool_calls_used`

## Available business capabilities

The assistant can:

- read current inventory and recent demand for a SKU;
- list supplier lead times and minimum order quantities;
- simulate whether a proposed order covers forecast demand.

Implement deterministic in-memory versions of these capabilities for the
prototype. The default project must not require credentials or an external
business service.

The assistant must inspect inventory and suppliers before recommending an
order. It may request multiple independent pieces of information together.

If it requests an unknown capability, supplies invalid arguments, receives a
capability error, or produces an invalid final result, return the error to the
assistant so it can correct itself. Allow at most one correction for each such
error and return a clear failure after the limit. The assistant must never
continue indefinitely.

The default automated behavior must be reproducible without contacting a paid
AI service. Document separately how a user can connect a compatible real model
for manual use.

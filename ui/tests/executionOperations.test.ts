import { test } from 'node:test';
import assert from 'node:assert/strict';
import { observeOperation, operationStateMatches, pendingOperation, readPendingOperation } from '../src/executionOperations.js';
import type { ExecutionControls, ExecutionOperation } from '../src/types.js';

const controls = (extra: Partial<ExecutionControls> = {}): ExecutionControls => ({
  invocation_id:'root',root_invocation_id:'root',root_session_id:'session',status:'waiting',control_state:'active',
  recovery_reason:null,expected_version:4,recovery_count:0,endpoints:[],pending_responses:[],next_cursor:null,frontier:4,
  actions: Object.fromEntries(['pause','recover','cancel','respond','signal','run_again'].map(action=>[action,{available:true,reason:null}])) as ExecutionControls['actions'],...extra,
});
const missing = new Error('missing');
const unavailable = (error: unknown) => error === missing;

test('pending observation storage contains identifiers but no submitted business data',()=>{
  const pending = pendingOperation({invocation_id:'root',request_id:'request',action:'respond',target_invocation_id:'child',wait_id:'wait',payload:{secret:'private'},reason:'private'});
  assert.deepEqual(pending,{operation_id:'request',action:'respond',target_invocation_id:'child',wait_id:'wait'});
  assert.deepEqual(readPendingOperation(JSON.stringify(pending)),pending);
  assert.deepEqual(readPendingOperation('old-request'),{operation_id:'old-request'});
  for(const value of [null,'{broken','{}','{"operation_id":1}'])assert.equal(readPendingOperation(value),null);
});

test('a retained receipt preserves real operation errors without inferring from execution state',async()=>{
  const receipt: ExecutionOperation={operation_id:'request',invocation_id:'root',action:'cancel',status:'failed',result:null,error:{code:'rejected',message:'Action rejected'}};
  const result = await observeOperation({operation_id:'request'}, {receipt:async()=>receipt,controls:async()=>{throw new Error('No reconciliation needed');},unavailable});
  assert.deepEqual(result,{kind:'receipt',receipt});
});

test('missing receipts reconcile paused and cancelled states without resubmitting any command',async()=>{
  for(const [action,current] of [['pause',controls({control_state:'paused'})],['cancel',controls({status:'cancelled'})]] as const){
    let reads=0;
    const result=await observeOperation({operation_id:'request',action},{receipt:async()=>{throw missing;},controls:async()=>{reads++;return current;},unavailable});
    assert.deepEqual(result,{kind:'reconciled',controls:current,message:null});
    assert.equal(reads,1);
  }
});

test('Recover reconciliation requires the exact durable request ID, even if another recovery already ran',()=>{
  const pending={operation_id:'request',action:'recover' as const};
  assert.equal(operationStateMatches(pending,controls({last_recovery_id:'request'})),true);
  assert.equal(operationStateMatches(pending,controls({last_recovery_id:'other',recovery_count:2})),false);
  assert.equal(operationStateMatches(pending,controls()),false);
});

test('response reconciliation checks the exact resumed Child Wait rather than a paginated notification list',()=>{
  const pending={operation_id:'request',action:'respond' as const,target_invocation_id:'child',wait_id:'wait'};
  const state=controls({next_cursor:'more',response_target:{invocation_id:'child',wait_id:'wait',status:'resumed'}});
  assert.equal(operationStateMatches(pending,state),true);
  for(const response_target of [null,{invocation_id:'other',wait_id:'wait',status:'resumed'},{invocation_id:'child',wait_id:'other',status:'resumed'},
    {invocation_id:'child',wait_id:'wait',status:'cancelled'},{invocation_id:'child',wait_id:'wait',status:'waiting'}]){
    assert.equal(operationStateMatches(pending,{...state,response_target}),false);
  }
});

test('continuing a paused or interrupted graph requires a newer active state rather than a terminal recovery marker',()=>{
  for(const reason of ['paused','interrupted'] as const){
    const pending=pendingOperation({invocation_id:'root',request_id:'request',action:'recover',expected_version:4},controls({recovery_reason:reason}));
    assert.deepEqual(readPendingOperation(JSON.stringify(pending)),pending);
    assert.equal(operationStateMatches(pending,controls({expected_version:5})),true);
    assert.equal(operationStateMatches(pending,controls({expected_version:4})),false);
    assert.equal(operationStateMatches(pending,controls({expected_version:5,recovery_reason:'interrupted'})),false);
    assert.equal(operationStateMatches(pending,controls({expected_version:5,control_state:'paused',recovery_reason:'paused'})),false);
  }
});

test('unaccepted submissions, unknown old IDs and Signal receipts remain explicitly unconfirmed',async()=>{
  for(const pending of [{operation_id:'request'},{operation_id:'request',action:'cancel' as const},{operation_id:'request',action:'signal' as const}]){
    const result=await observeOperation(pending,{receipt:async()=>{throw missing;},controls:async()=>controls(),unavailable});
    assert.equal(result.kind,'reconciled');
    if(result.kind==='reconciled')assert.match(result.message!,/could not be confirmed/);
  }
});

test('network failures keep observation retryable and never turn into a successful reconciliation',async()=>{
  const offline=new Error('offline');
  await assert.rejects(observeOperation({operation_id:'request'},{receipt:async()=>{throw offline;},controls:async()=>controls(),unavailable}),/offline/);
  await assert.rejects(observeOperation({operation_id:'request'},{receipt:async()=>{throw missing;},controls:async()=>{throw offline;},unavailable}),/offline/);
});

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ObservationClock, observationInterval, TimelineActivity, appendPauseIntervals, pauseIntervals, pausedDuration } from '../src/liveObservation.js';
import { moveWindow, spanGeometry } from '../src/timeline.js';
import type { ExecutionTrace, TimelineMarker } from '../src/types.js';

const trace=(status:string,control='active'):ExecutionTrace=>({invocation:{invocation_id:'i',root_invocation_id:'i',workflow_id:'wf',workflow_revision_id:'r',values:[],status,control_state:control},observed_at_us:100,last_event:null,nodes:[],edges:[],active_routes:[],child_count:0,frontier:1,view:'durable'});
test('the independent clock advances monotonically, holds Replay calibration and unsubscribes without notifying the workspace',()=>{
  let local=0,updates=0;
  const clock=new ObservationClock(1000,()=>local),unsubscribe=clock.subscribe(()=>updates++);
  local=10;clock.tick();assert.equal(clock.get(),11000);assert.equal(updates,1);
  clock.calibrate(9000);assert.equal(clock.get(),11000);
  clock.calibrate(20000,false);assert.equal(clock.get(),11000);
  local=11;clock.tick();assert.equal(clock.get(),21000);
  unsubscribe();local=12;clock.tick();assert.equal(updates,2);
});
test('waits and pauses use a slower clock, terminal states stop it and concurrent work remains active',()=>{
  assert.equal(observationInterval(trace('running')),500);
  assert.equal(observationInterval(trace('waiting')),1000);
  assert.equal(observationInterval(trace('running','paused')),1000);
  for(const status of ['completed','failed','cancelled'])assert.equal(observationInterval(trace(status)),0);
  const concurrent=trace('settling');concurrent.nodes=[{node_id:'n',occurrence_id:'o',occurrences:1,status:'running'}];
  assert.equal(observationInterval(concurrent),500);
});
test('idle geometry is derived from committed boundaries and does not move with observer time',()=>{
  const activity=new TimelineActivity();
  activity.append([{event_id:'wait',sequence:2,event_name:'node_occurrence.waiting',occurred_at_us:120,invocation_status:'waiting'}]);
  const waiting=trace('waiting');
  const idle=activity.idle(waiting,100,[]);
  assert.deepEqual(idle,{state:'waiting',since_at_us:120,at_us:120});
  assert.deepEqual(activity.idle(waiting,100,[]),idle);
  activity.append([{event_id:'reply',sequence:3,event_name:'node_occurrence.completed',occurred_at_us:200}]);
  assert.deepEqual(activity.idle(waiting,150,[]),{state:'waiting',since_at_us:120,at_us:200});
  assert.equal(activity.idle(trace('running'),150,[]),null);
  const pause=[{start_at_us:250,end_at_us:null}];
  activity.append([{event_id:'pause',sequence:4,event_name:'graph.paused',occurred_at_us:250}]);
  assert.deepEqual(activity.idle(trace('waiting','paused'),200,pause),{state:'paused',since_at_us:250,at_us:250});
  assert.equal(activity.idle(trace('cancelled','paused'),200,pause),null);
  assert.deepEqual(moveWindow({start:0,end:2000},20,30),{start:20,end:50});
});
test('control intervals close on resume or cancellation, preserve sequence order and never subtract waiting time',()=>{
  const event=(sequence:number,at:number,control_state?:string,invocation_status?:string):TimelineMarker=>({event_id:String(sequence),event_name:'invocation.changed',sequence,occurred_at_us:at,control_state,invocation_status});
  const first=[event(1,10,'paused'),event(2,12,'paused'),event(3,20,'active')];
  let pauses=pauseIntervals(first);
  assert.deepEqual(pauses,[{start_at_us:10,end_at_us:20}]);
  const next=[event(4,30,'paused'),event(5,50,undefined,'cancelled')];
  pauses=appendPauseIntervals(pauses,next);
  assert.deepEqual(pauses,pauseIntervals([...first,...next]));
  assert.equal(pausedDuration(pauses,15,40),15);
  const waiting=spanGeometry({start_at_us:0,end_at_us:null,status:'waiting'},{start:0,end:40},60);
  assert.equal(waiting.elapsed,60);assert.equal(waiting.clippedEnd,true);
  assert.equal(pausedDuration(pauseIntervals([event(1,10,'paused')]),0,40),30);
  // Reversed wall timestamps are clamped without changing sequence order.
  assert.deepEqual(pauseIntervals([event(1,20,'paused'),event(2,10,'active')]),[{start_at_us:20,end_at_us:20}]);
});

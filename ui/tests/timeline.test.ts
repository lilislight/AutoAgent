import { test } from 'node:test';
import assert from 'node:assert/strict';
import { eventCategory, focusWindow, fullWindow, moveWindow, resizeWindow, selectedWindow, spanGeometry, elapsedLabel, timelineScale, timelineTimestamp, zoomWindow } from '../src/timeline.js';
import type { TimelineOverview, TimelineRow } from '../src/types.js';
const origin = 1_780_000_000_000_000;
function row(start: number, end: number | null, status = 'completed'): TimelineRow {
  return { id:'occ',kind:'node',invocation_id:'root',occurrence_id:'occ',start_at_us:origin+start,end_at_us:end===null?null:origin+end,status,values:[] };
}
const window = { start:origin+100,end:origin+200 };
test('microsecond geometry clips crossing spans without changing actual duration', () => {
  const geometry=spanGeometry(row(50,250),window,origin+300);
  assert.equal(geometry.left,0);assert.equal(geometry.width,100);assert.equal(geometry.elapsed,200);
  assert.equal(geometry.clippedStart,true);assert.equal(geometry.clippedEnd,true);
  const subpixel=spanGeometry(row(101,102),window,origin+300);assert.equal(subpixel.width,1);
});
test('half-open windows include skipped points and exclude prior completions', () => {
  assert.equal(spanGeometry(row(0,100),window,origin+300).visible,false);
  assert.equal(spanGeometry(row(100,100,'skipped'),window,origin+300).visible,true);
  assert.equal(spanGeometry(row(200,200,'skipped'),window,origin+300).visible,false);
});
test('open Waits use the observation boundary while unknown terminal ends stay unknown', () => {
  const waiting=spanGeometry(row(50,null,'waiting'),window,origin+180);
  assert.equal(waiting.open,true);assert.equal(waiting.width,80);assert.equal(waiting.elapsed,130);
  const cancelled=spanGeometry(row(110,null,'cancelled'),window,origin+180);
  assert.equal(cancelled.point,true);assert.equal(cancelled.elapsed,null);
});
test('range navigation stays within bounds even for long or zero-duration histories', () => {
  const full={start:origin,end:origin+654_000_000};
  assert.deepEqual(moveWindow(full,origin-100,500),{start:origin,end:origin+500});
  assert.deepEqual(moveWindow(full,full.end,800),{start:full.end-800,end:full.end});
  assert.deepEqual(moveWindow(full,origin,1_000_000_000),full);
  const zero={start_at_us:origin,end_at_us:origin} as TimelineOverview;
  assert.deepEqual(fullWindow(zero),{start:origin,end:origin+1});
  assert.equal(elapsedLabel(640_000_000),'10m 40s');
});
test('overview selection works in either direction and handles never cross', () => {
  const full={start:origin,end:origin+1000};
  assert.deepEqual(selectedWindow(full,origin+800,origin+200),{start:origin+200,end:origin+800});
  const selected={start:origin+200,end:origin+800};
  assert.deepEqual(resizeWindow(full,selected,'start',origin+999),{start:origin+799,end:origin+800});
  assert.deepEqual(resizeWindow(full,selected,'end',origin+100),{start:origin+200,end:origin+201});
  assert.deepEqual(resizeWindow(full,selected,'end',origin+2000),{start:origin+200,end:full.end});
});
test('focusing a Call includes its interval and context, including short and boundary intervals', () => {
  const full={start:origin,end:origin+10000};
  assert.deepEqual(focusWindow(full,row(2000,4000),full.end),{start:origin+1800,end:origin+4200});
  assert.deepEqual(focusWindow(full,row(0,100),full.end),{start:origin,end:origin+1000});
  assert.deepEqual(focusWindow(full,row(9999,9999),full.end),{start:origin+9000,end:full.end});
  assert.deepEqual(focusWindow(full,row(0,null,'waiting'),full.end),full);
});

test('one scale unit covers the whole axis and timestamps retain microsecond precision', () => {
  assert.deepEqual(timelineScale({ start:origin,end:origin+400_214 }),{unit:'ms',divisor:1000});
  assert.deepEqual(timelineScale({ start:origin,end:origin+10_000_000 }),{unit:'s',divisor:1_000_000});
  assert.deepEqual(timelineScale({ start:origin,end:origin+654_000_000 }),{unit:'min',divisor:60_000_000});
  assert.match(timelineTimestamp(origin+590155),/\.590155Z$/);
  assert.equal(elapsedLabel(1543 / 1000), '1.54 µs');
});
test('Call bars follow Event boundaries independently of monotonic execution duration', () => {
  const call={...row(174855,213400),kind:'call' as const,execution_duration_ns:25_242_642};
  const view=spanGeometry(call,{start:origin,end:origin+400214},origin+400214);
  assert.equal(view.elapsed,38545);
  assert.equal(view.width,38545/400214*100);
  assert.notEqual(view.elapsed,call.execution_duration_ns/1000);
});
test('pointer-anchored zoom preserves the inspected instant and clamps to execution bounds', () => {
  const full={start:origin,end:origin+10000};
  const current={start:origin+2000,end:origin+6000};
  const zoomed=zoomWindow(full,current,.5,.25);
  assert.deepEqual(zoomed,{start:origin+2500,end:origin+4500});
  assert.equal(zoomed.start+(zoomed.end-zoomed.start)*.25, current.start+(current.end-current.start)*.25);
  assert.deepEqual(zoomWindow(full,current,10,.25),full);
  assert.deepEqual(zoomWindow(full,{start:origin,end:origin+1000},.001,0),{start:origin,end:origin+1});
});
test('Event colours distinguish domains without hiding faults', () => {
  assert.equal(eventCategory('node_occurrence.waiting'), 'node');
  assert.equal(eventCategory('operator_call.completed'), 'call');
  assert.equal(eventCategory('child_invocation.phase_changed'), 'invocation');
  assert.equal(eventCategory('routing.resolved'), 'routing');
  assert.equal(eventCategory('wait.resumed'), 'wait');
  assert.equal(eventCategory('input.mapped'), 'data');
  assert.equal(eventCategory('future_domain.changed'), 'runtime');
  assert.equal(eventCategory('operator_call.failed'), 'error');
});

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { eventZoomWindow, focusRange, initialRange, panRange, previousRange, rememberRange, resolveRange, selectRange, zoomRange } from '../src/timelineNavigation.js';
import { elapsedPosition } from '../src/timeline.js';
const start=1_780_000_000_000_000;
const full={start,end:start+12*60*60*1_000_000};
test('late submillisecond ranges keep distinct exact axis labels',()=>{
  assert.equal(elapsedPosition(43_200_000_001,4),'12:00:00.000001');
  assert.equal(elapsedPosition(43_200_000_002,4),'12:00:00.000002');
  assert.equal(elapsedPosition(59_999_999,10_000),'1:00.000');
});
test('successive brush expansion resolves millisecond work after a twelve hour Wait and Back restores both ranges',()=>{
  let nav=initialRange();
  nav=selectRange(nav,full,{start:full.end-1_000_000,end:full.end});
  const selected=nav;
  nav=focusRange(nav,full,resolveRange(full,nav.frame).window);
  assert.deepEqual(resolveRange(full,nav.frame),{domain:{start:full.end-1_000_000,end:full.end},window:{start:full.end-1_000_000,end:full.end}});
  nav=selectRange(nav,full,{start:full.end-4000,end:full.end-2000});
  const narrow=nav;
  nav=focusRange(nav,full,resolveRange(full,nav.frame).window);
  assert.equal(resolveRange(full,nav.frame).domain.end-resolveRange(full,nav.frame).domain.start,2000);
  assert.deepEqual(previousRange(nav).frame,narrow.frame);
  assert.deepEqual(previousRange(previousRange(previousRange(nav))).frame,selected.frame);
  nav=rememberRange(nav,{domain:null,window:null},full);
  assert.deepEqual(resolveRange(full,nav.frame),{domain:full,window:full});
});
test('zoom remains pointer anchored and bottoms out at one microsecond without splitting equal timestamps',()=>{
  const local={start:start+10000,end:start+20000};
  const nav=zoomRange(focusRange(initialRange(),full,local),full,.5,.25);
  assert.deepEqual(resolveRange(full,nav.frame).domain,{start:start+11250,end:start+16250});
  const micro=resolveRange(full,zoomRange(nav,full,.000001).frame).window;
  assert.equal(micro.end-micro.start,1);
  assert.equal(eventZoomWindow(full,start+100,start+101),null);
  assert.deepEqual(eventZoomWindow(full,start+100,start+201),{start:start+85,end:start+216});
});
test('pan keeps the brush relative to its domain and a gesture adds only one history entry',()=>{
  let nav=focusRange(initialRange(),full,{start:start+10000,end:start+20000});
  nav=selectRange(nav,full,{start:start+13000,end:start+15000});
  const before=nav;
  nav=panRange(nav,full,start+20000);
  nav=panRange(nav,full,start+30000,false);
  assert.deepEqual(resolveRange(full,nav.frame),{domain:{start:start+30000,end:start+40000},window:{start:start+33000,end:start+35000}});
  assert.deepEqual(previousRange(nav).frame,before.frame);
  const bounded=resolveRange(full,panRange(nav,full,full.end).frame);
  assert.equal(bounded.domain.end,full.end);
  assert.equal(bounded.window.end,bounded.domain.start+5000);
});
test('live append extends an unzoomed overview but preserves manual local ranges',()=>{
  const grown={start,end:full.end+10000};
  assert.deepEqual(resolveRange(grown,initialRange().frame).window,grown);
  const brushed=selectRange(initialRange(),full,{start:start+50,end:start+200});
  assert.deepEqual(resolveRange(grown,brushed.frame),{domain:grown,window:{start:start+50,end:start+200}});
  const focused=focusRange(brushed,full,{start:start+100,end:start+1000});
  assert.deepEqual(resolveRange(full,focused.frame),resolveRange(grown,focused.frame));
});

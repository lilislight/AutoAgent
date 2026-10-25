import { useEffect, useMemo, useRef } from 'react';
import { familyCurve, familyTargetVisible, type FamilyPoint } from '../familyHandoff';
import type { FamilyHandoff } from '../types';
import type { HandoffCue } from './useFamilyHandoff';
import { useFamilyPresence } from './useFamilyPresence';

function visibleRect(element:Element|null,clip:Element) {
  if(!element || element.closest('[inert]'))return null;
  const rect=element.getBoundingClientRect(),bounds=clip.getBoundingClientRect();
  return rect.width && rect.height && rect.bottom>bounds.top && rect.top<bounds.bottom && rect.right>bounds.left && rect.left<bounds.right?rect:null;
}
function origin(workspace:Element,item:FamilyHandoff):{rect:DOMRect;element:Element}|null {
  const graph=workspace.querySelector('.execution-graph-pane:not([aria-hidden="true"])');
  if(graph){
    const node=[...graph.querySelectorAll<HTMLElement>('[data-family-node]')].find(el=>el.dataset.familyNode===item.nodeId);
    const rect=visibleRect(node??null,graph);
    if(rect)return {rect,element:node!};
    // A folded inline Workflow represents its hidden source Nodes.
    const group=[...graph.querySelectorAll<HTMLElement>('[data-family-node-ids]')].find(el=>{
      const ids=JSON.parse(el.dataset.familyNodeIds??'[]') as string[];return ids.includes(item.nodeId);
    });
    const bounds=visibleRect(group??null,graph);if(bounds)return{rect:bounds,element:group!};
  }
  const timeline=workspace.querySelector('.execution-timeline-pane:not([aria-hidden="true"])');
  if(timeline){
    const rows=[...timeline.querySelectorAll<HTMLElement>('[data-timeline-id]')];
    for(const id of [item.waitId,item.occurrenceId]){
      const row=rows.find(el=>el.dataset.timelineId===id),block=row?.querySelector('.timeline-block-hit');
      const rect=visibleRect(block??null,timeline);
      if(rect)return{rect,element:block!};
    }
  }
  return null;
}

/** Screen-space feedback shared by Graph and Timeline, above the Inspector. */
export function FamilyHandoffOverlay({cue,onFinish}:{cue:HandoffCue|null;onFinish:(key:string)=>void}) {
  const surface=useRef<SVGSVGElement>(null);
  const template=useRef<SVGGElement>(null),layer=useRef<SVGGElement>(null);
  const byTarget=useMemo(()=>{
    const targets=new Map<string,FamilyHandoff[]>();
    for(const item of cue?.handoffs??[]){
      const items=targets.get(item.target.invocation_id)??[];
      items.push(item);targets.set(item.target.invocation_id,items);
    }
    return targets;
  },[cue]);
  const latest=useRef(byTarget);latest.current=byTarget;
  const {hold}=useFamilyPresence();
  useEffect(()=>{
    if(!cue || !surface.current || !layer.current || !template.current)return;
    const svg=surface.current,drawing=layer.current,prototype=template.current,workspace=svg.closest('.flow-loading-content');
    if(!workspace)return;
    const groups=new Map<string,SVGGElement>();
    const release=hold(),reduced=matchMedia('(prefers-reduced-motion: reduce)').matches;
    let frame=0,start:number|undefined,cancelled=false;
    const born=performance.now();
    const highlights=new Set<Element>();
    const duration=reduced?550:1650;
    const cleanup=()=>{cancelled=true;cancelAnimationFrame(frame);drawing.replaceChildren();highlights.forEach(el=>el.classList.remove('is-handoff-source','is-handoff-target'));release();};
    const tick=(now:number)=>{
      if(cancelled)return;
      start??=now;
      const elapsed=now-start;
      const box=svg.getBoundingClientRect();
      const visible:{item:FamilyHandoff;target:HTMLElement;end:DOMRect}[]=[];
      // Iterate the bounded, mounted card page, not every Child in a large Map.
      for(const target of workspace.querySelectorAll<HTMLElement>('[data-family-invocation]')){
        const items=latest.current.get(target.dataset.familyInvocation!),strip=target.closest('.family-card-strip');
        if(!items || !strip)continue;
        const end=visibleRect(target,strip);
        if(!end || !familyTargetVisible(end,strip.getBoundingClientRect(),box))continue;
        for(const item of items)visible.push({item,target,end});
      }
      const visibleKeys=new Set(visible.map(({item})=>item.key));
      for(const [key,group] of groups)if(!visibleKeys.has(key)){group.remove();groups.delete(key);}
      for(const element of highlights)if(element.classList.contains('is-handoff-target') && !visible.some(({target})=>target===element)){
        element.classList.remove('is-handoff-target');highlights.delete(element);
      }
      visible.forEach(({item,target,end})=>{
        let group=groups.get(item.key);
        if(!group){group=prototype.cloneNode(true) as SVGGElement;group.dataset.targetInvocation=item.target.invocation_id;drawing.append(group);groups.set(item.key,group);}
        const from=origin(workspace,item),above=from?from.rect.y+from.rect.height/2<=end.y+end.height/2:true;
        const point:FamilyPoint={x:end.x+end.width/2-box.x,y:(above?end.top:end.bottom)-box.y};
        const fade=reduced?1:Math.max(0,Math.min(1,(elapsed-260)/100))*Math.min(1,(duration-elapsed)/450);
        group.style.opacity=String(Math.max(0,fade));
        if(elapsed>400){target.classList.add('is-handoff-target');highlights.add(target);}
        if(from){from.element.classList.add('is-handoff-source');highlights.add(from.element);}
        const paths=[group.children[0],group.children[1]] as SVGPathElement[];
        const dot=group.children[2] as SVGCircleElement;
        const text=group.children[3] as SVGTextElement;
        text.setAttribute('x',String(Math.max(80,Math.min(box.width-80,point.x))));text.setAttribute('y',String(Math.max(18,point.y-14)));
        text.textContent=visible.length===1?(item.kind==='spawn'?'Child created':'Awaiting child'):'';
        if(!from || reduced){paths.forEach(path=>path.style.opacity='0');dot.style.opacity='0';return;}
        const a={x:from.rect.x+from.rect.width*.65-box.x,y:Math.max(8,Math.min((above?from.rect.bottom:from.rect.top)-box.y,box.height-8))};
        const curve=familyCurve(a,point,box.width);
        paths.forEach(path=>{path.setAttribute('d',curve);path.style.opacity='1';});
        const progress=Math.max(0,Math.min(1,(elapsed-280)/650)),eased=1-Math.pow(1-progress,2);
        paths[1].style.strokeDashoffset=String(elapsed>duration-450?-(elapsed-duration+450)/450:1-eased);
        paths[0].style.opacity=String(progress*.12);
        if(progress>0 && progress<1){const at=paths[1].getPointAtLength(paths[1].getTotalLength()*eased);dot.setAttribute('cx',String(at.x));dot.setAttribute('cy',String(at.y));dot.style.opacity='1';}else dot.style.opacity='0';
      });
      if(!visible.length){if(now-born<1200){start=now;frame=requestAnimationFrame(tick);return;}cleanup();onFinish(cue.key);return;}
      if(elapsed<duration)frame=requestAnimationFrame(tick);
      else {cleanup();onFinish(cue.key);}
    };
    frame=requestAnimationFrame(tick);
    const escape=(event:KeyboardEvent)=>{if(event.key==='Escape'){cleanup();onFinish(cue.key);}};
    document.addEventListener('keydown',escape);
    return()=>{cleanup();document.removeEventListener('keydown',escape);};
  },[cue?.key,hold,onFinish]);
  return <svg ref={surface} className="family-handoff-overlay" aria-hidden="true"><defs><g ref={template} opacity="0"><path className="family-handoff-glow"/><path className="family-handoff-line" pathLength="1"/><circle className="family-handoff-dot" r="3"/><text className="family-handoff-label" textAnchor="middle"/></g></defs><g ref={layer}/></svg>;
}

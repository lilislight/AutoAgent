import type { FamilyHandoff, FamilyLink } from './types.js';

/** Immutable relationship facts, indexed independently of the million-Event track. */
export class FamilyLinkIndex {
  private ids=new Set<string>();
  private rows:FamilyLink[]=[];
  private childIds=new Set<string>();
  private childRows:FamilyLink['target'][]=[];
  add(rows:FamilyLink[]) {
    const fresh=rows.filter(row=>{if(this.ids.has(row.id))return false;this.ids.add(row.id);return true;});
    if(!fresh.length)return fresh;
    for(const row of fresh)this.rows.push(row);
    this.rows.sort((a,b)=>a.sequence-b.sequence || a.occurrence_id.localeCompare(b.occurrence_id) || Number(a.target.unit_index??0)-Number(b.target.unit_index??0) || a.id.localeCompare(b.id));
    let added=false;
    for(const row of fresh)if(!this.childIds.has(row.target.invocation_id)){this.childIds.add(row.target.invocation_id);this.childRows.push(row.target);added=true;}
    if(added)this.childRows.sort((a,b)=>a.created_sequence-b.created_sequence || Number(a.unit_index??0)-Number(b.unit_index??0) || a.invocation_id.localeCompare(b.invocation_id));
    return fresh;
  }
  children(through:number,cursor:string|null=null) {
    const offset=Number(cursor??0);
    if(!Number.isSafeInteger(offset) || offset<0)throw new Error('Invalid local family cursor.');
    let lo=0,hi=this.childRows.length;
    while(lo<hi){const mid=(lo+hi)>>>1;if(this.childRows[mid].created_sequence<=through)lo=mid+1;else hi=mid;}
    return {items:this.childRows.slice(offset,Math.min(lo,offset+20)),next_cursor:offset+20<lo?String(offset+20):null};
  }
  private lower(sequence:number) {
    let lo=0,hi=this.rows.length;
    while(lo<hi){const mid=(lo+hi)>>>1;if(this.rows[mid].sequence<sequence)lo=mid+1;else hi=mid;}
    return lo;
  }
  handoffs({after,through,exact=false}:{after:number;through:number;exact?:boolean}):FamilyHandoff[] {
    const groups=new Map<string,FamilyHandoff>();
    for(let i=this.lower(exact?through:after+1);i<this.rows.length && this.rows[i].sequence<=through;i++){
      const row=this.rows[i];
      if(row.animate===false)continue;
      if(row.target.created_sequence>through)continue;
      // Coalesce duplicate facts about one Child, never distinct Children of a Map.
      const key=JSON.stringify([row.occurrence_id,row.target.invocation_id]);
      groups.set(key,{key,kind:row.kind,sequence:row.sequence,occurrenceId:row.occurrence_id,
        nodeId:row.node_id,waitId:row.wait_id,target:row.target});
    }
    return [...groups.values()].sort((a,b)=>b.sequence-a.sequence);
  }
}

export interface FamilyPoint {x:number;y:number}
/** A screen-space Bezier. Both ends approach vertically, away from label text. */
export function familyCurve(source:FamilyPoint,target:FamilyPoint,width:number) {
  const distance=Math.abs(target.y-source.y),bend=Math.min(170,Math.max(48,distance*.45));
  const direction=target.x>=source.x?1:-1;
  const vertical=target.y>=source.y?1:-1;
  const x=Math.max(8,Math.min(width-8,source.x+direction*Math.min(60,Math.abs(target.x-source.x)*.3+28)));
  return `M ${source.x} ${source.y} C ${x} ${source.y+vertical*bend}, ${target.x} ${target.y-vertical*bend}, ${target.x} ${target.y}`;
}


interface FamilyBounds {left:number;right:number;top:number;bottom:number}
/** Hidden or clipped cards are not animation destinations. */
export function familyTargetVisible(card:FamilyBounds,...clips:FamilyBounds[]) {
  return card.right>card.left && card.bottom>card.top && clips.every(clip=>
    card.left>=clip.left && card.right<=clip.right && card.top>=clip.top && card.bottom<=clip.bottom);
}

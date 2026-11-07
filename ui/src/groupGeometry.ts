import type { GraphDefinition } from './types.js';
import type { Layout, Point } from './layout.js';
import { routePortPairs, type PortPair, type GraphRect, GRAPH_NODE_WIDTH, GRAPH_NODE_HEIGHT } from './graphRouting.js';
import { definitionPorts, edgePortId } from './graphPorts.js';
import { groupNodeId, workflowGroups } from './workflowGroups.js';
export function rerouteGroups(definition: GraphDefinition, folded: ReadonlySet<string>, layout: Layout): Layout['routes'] {
  const visible = workflowGroups(definition,folded), pairs:PortPair[]=[];
  const ports=layout.ports??definitionPorts(visible);
  const ancestors=(id:string)=>{const result=new Set<string>();let p=layout.parents?.[id];while(p){result.add(p);p=layout.parents?.[p];}return result;};
  const rectangle=(id:string):GraphRect=>{const p=layout.positions[id],s=layout.sizes?.[id]??{width:GRAPH_NODE_WIDTH,height:GRAPH_NODE_HEIGHT};return {left:p.x-10,top:p.y-10,right:p.x+s.width+10,bottom:p.y+s.height+10};};
  for(const edge of visible.edges){
    const source=layout.positions[edge.source],target=layout.positions[edge.target];if(!source||!target)continue;
    const sourceSize=layout.sizes?.[edge.source]??{width:GRAPH_NODE_WIDTH,height:GRAPH_NODE_HEIGHT};
    const targetSize=layout.sizes?.[edge.target]??{width:GRAPH_NODE_WIDTH,height:GRAPH_NODE_HEIGHT};
    const sourcePorts=visible.ports.get(edge.source.slice('@group:'.length)),targetPorts=visible.ports.get(edge.target.slice('@group:'.length));
    const from={x:source.x+sourceSize.width,y:source.y+(sourcePorts?.find(p=>p.id===edge.sourceHandle)?.y??ports[edge.source]?.find(p=>p.id===edgePortId(edge.id,'out'))?.y??sourceSize.height/2)};
    const to={x:target.x,y:target.y+(targetPorts?.find(p=>p.id===edge.targetHandle)?.y??ports[edge.target]?.find(p=>p.id===edgePortId(edge.id,'in'))?.y??targetSize.height/2)};
    const sourceAncestors=ancestors(edge.source),targetAncestors=ancestors(edge.target);
    const own=new Set([...sourceAncestors,...targetAncestors]);
    const common=[...sourceAncestors].find(id=>targetAncestors.has(id));
    const owner=common?rectangle(common):undefined;
    const bounds=owner?{left:owner.left+14,top:owner.top+54,right:owner.right-14,bottom:owner.bottom-14}:undefined;
    const obstacles=Object.keys(layout.positions).filter(id=>!own.has(id)).map(rectangle);
    for(const id of own){const r=rectangle(id);obstacles.push({...r,bottom:r.top+48});}
    pairs.push({id:edge.id,source:from,target:to,obstacles,bounds});
  }
  return routePortPairs(pairs);
}
/** Fallback preserves ownership even when the layout worker is unavailable. */
export function fallbackGroups(definition: GraphDefinition, folded: ReadonlySet<string>): Layout {
  const visible=workflowGroups(definition,folded), positions:Layout['positions']={},sizes:NonNullable<Layout['sizes']>={},parents:NonNullable<Layout['parents']>={};
  function place(owner:string|null,at:Point):{width:number;height:number}{
    const objects=[...visible.nodes.filter(n=>(visible.owners.get(n.id)??null)===owner).map(n=>({id:n.id,group:null as string|null})),...visible.groups.filter(g=>g.parent_group_id===owner).map(g=>({id:groupNodeId(g.id),group:g.id}))];
    let x=at.x+28,height=0;
    for(const item of objects){
      const point={x,y:at.y+54};positions[item.id]=point;
      if(owner)parents[item.id]=groupNodeId(owner);
      const size=item.group&&!folded.has(item.group)?place(item.group,point):visible.sizes.get(item.id)??{width:GRAPH_NODE_WIDTH,height:GRAPH_NODE_HEIGHT};
      sizes[item.id]=size;x+=size.width+80;height=Math.max(height,size.height);
    }
    return {width:Math.max(GRAPH_NODE_WIDTH,x-at.x-80+28),height:height+82};
  }
  place(null,{x:0,y:0});const layout={positions,sizes,parents,ports:definitionPorts(visible),routes:{}};layout.routes=rerouteGroups(definition,folded,layout);return layout;
}

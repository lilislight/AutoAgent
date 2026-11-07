import type { GraphDefinition } from './types.js';
import { GRAPH_NODE_HEIGHT } from './graphRouting.js';

export interface GraphPort { id:string; side:'in'|'out'; y:number }
export type GraphPorts = Record<string,GraphPort[]>;
export const edgePortId = (id:string,side:'in'|'out') => JSON.stringify(['edge',side,id]);

/** Each Edge owns its attachment; independent branches must not become a bus. */
export function definitionPorts(definition:Pick<GraphDefinition,'nodes'|'edges'>):GraphPorts {
  const ports:GraphPorts=Object.fromEntries(definition.nodes.map(node=>[node.id,[]]));
  for(const edge of definition.edges){
    ports[edge.source]?.push({id:edgePortId(edge.id,'out'),side:'out',y:0});
    ports[edge.target]?.push({id:edgePortId(edge.id,'in'),side:'in',y:0});
  }
  for(const list of Object.values(ports))for(const side of ['in','out'] as const){
    const peers=list.filter(p=>p.side===side);
    peers.forEach((p,index)=>p.y=peers.length===1?GRAPH_NODE_HEIGHT/2:24+(GRAPH_NODE_HEIGHT-48)*index/(peers.length-1));
  }
  return ports;
}

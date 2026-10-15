/** Optional disk cache. A failed write never turns a partial download into a cache hit. */
const DB='autoagent.trace-cache.v2';
const MAX_BYTES=256*1024*1024;
let opening:Promise<IDBDatabase|null>|undefined;
function database(){
  return opening??=new Promise<IDBDatabase|null>(resolve=>{
    if(typeof indexedDB==='undefined'){resolve(null);return;}
    let request:IDBOpenDBRequest;
    try{request=indexedDB.open(DB,2);}catch{resolve(null);return;}
    request.onupgradeneeded=()=>{for(const name of ['datasets','metadata'])if(!request.result.objectStoreNames.contains(name))request.result.createObjectStore(name);};
    request.onsuccess=()=>{request.result.onversionchange=()=>{request.result.close();opening=undefined;};resolve(request.result);};
    request.onerror=()=>resolve(null);request.onblocked=()=>resolve(null);
  });
}
export async function readTraceCache<T>(key:string):Promise<T|null>{
  try{const db=await database();if(!db)return null;return await new Promise<T|null>(resolve=>{
    const tx=db.transaction('datasets','readonly'),request=tx.objectStore('datasets').get(key);
    request.onsuccess=()=>resolve(request.result??null);request.onerror=()=>resolve(null);tx.onabort=()=>resolve(null);
  });}catch{return null;}
}
export async function writeTraceCache(key:string,value:unknown,bytes:number){
  if(bytes>MAX_BYTES)return;
  try{const db=await database();if(!db)return;await new Promise<void>(resolve=>{
    const tx=db.transaction(['datasets','metadata'],'readwrite'),store=tx.objectStore('datasets'),metadata=tx.objectStore('metadata');
    const request=metadata.getAll();request.onsuccess=()=>{
      const rows=request.result as {key:string;bytes:number;used:number}[];
      let total=rows.filter(row=>row.key!==key).reduce((sum,row)=>sum+row.bytes,0)+bytes;
      for(const row of rows.sort((a,b)=>a.used-b.used)){if(total<=MAX_BYTES)break;if(row.key!==key){store.delete(row.key);metadata.delete(row.key);total-=row.bytes;}}
      store.put(value,key);metadata.put({key,bytes,used:Date.now()},key);
    };tx.oncomplete=()=>resolve();tx.onerror=()=>resolve();tx.onabort=()=>resolve();
  });}catch{/* Private browsing, quota or policy: keep this page's memory cache. */}
}

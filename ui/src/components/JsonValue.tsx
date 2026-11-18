import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState, type FocusEvent, type PointerEvent } from 'react';
import { fieldWindow, jsonMatchRanges, jsonPath, jsonPreview, jsonTextPreview, pathStartsWith, searchJson, type JsonMatch, type JsonPath } from '../jsonInspection.js';
import { ToolButton, CheckIcon, CopyIcon, DownloadIcon, PathIcon, SearchValueIcon } from './ToolButton.js';
import { LoadingIndicator } from './LoadingState.js';
import { copyText } from '../clipboard.js';
import { ManualCopyText } from './ManualCopyText.js';

function useJsonClipboard(scope: unknown) {
  const [feedback, setFeedback] = useState<{ key: string; failed: boolean; manualText?: string } | null>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const attempt = useRef(0);
  useEffect(() => () => clearTimeout(timer.current), []);
  useEffect(() => { attempt.current++; clearTimeout(timer.current); setFeedback(null); }, [scope]);
  const dismiss = () => { attempt.current++; clearTimeout(timer.current); setFeedback(null); };
  const copy = async (trigger: HTMLButtonElement, key: string, value: unknown, path = false) => {
    clearTimeout(timer.current);
    const current = ++attempt.current;
    let manualText: string | undefined;
    let copied = false;
    try {
      manualText = path ? String(value) : JSON.stringify(value, null, 2) ?? 'undefined';
      copied = await copyText(manualText, trigger);
    } catch { /* JSON serialization failed. */ }
    if (current !== attempt.current) return;
    const manual = key === 'root' && !copied ? manualText : undefined;
    setFeedback({ key, failed: !copied, manualText: manual });
    if (manual === undefined) timer.current = setTimeout(() => setFeedback(null), 1600);
  };
  return { feedback, copy, dismiss };
}

function FieldActions({ value, path }: { value: unknown; path: JsonPath }) {
  const { feedback, copy } = useJsonClipboard(value), location = jsonPath(path);
  return <span className="json-node-actions" onClick={event => { event.stopPropagation(); }} onKeyDown={event => { if (event.key === 'Enter' || event.key === ' ') event.stopPropagation(); }}>
    <ToolButton label={`Copy value at ${location}`} tooltip={feedback?.key==='value' ? feedback.failed?'Copy failed':'Copied value' : `Copy value at ${location}`} onClick={event => { event.preventDefault(); void copy(event.currentTarget, 'value', value); }}>{feedback?.key==='value'&&!feedback.failed?<CheckIcon/>:<CopyIcon/>}</ToolButton>
    <ToolButton label={`Copy path ${location}`} tooltip={feedback?.key==='path' ? feedback.failed?'Copy failed':'Copied path' : location} onClick={event => { event.preventDefault(); void copy(event.currentTarget, 'path', location, true); }}>{feedback?.key==='path'&&!feedback.failed?<CheckIcon/>:<PathIcon/>}</ToolButton>
    <span className="sr-only" role="status">{feedback ? feedback.failed?'Copy failed. Select the content and copy it.':'Copied' : ''}</span>
  </span>;
}

export function JsonSearchText({ text, query }: { text: string; query: string }) {
  const ranges = jsonMatchRanges(text, query);
  if (!ranges.length) return <>{text}</>;
  let cursor = 0;
  const parts = ranges.map(({ start, end }) => {
    const prefix = text.slice(cursor, start); cursor = end;
    return <span key={start}>{prefix}<mark className="json-search-highlight">{text.slice(start, end)}</mark></span>;
  });
  return <>{parts}{text.slice(cursor)}</>;
}

function Scalar({ value, query }: { value: unknown; query: string }) {
  const [expanded, setExpanded] = useState(false);
  let text: string;
  try { text = JSON.stringify(value) ?? 'undefined'; } catch { text = String(value); }
  const long = text.length > 500;
  const escapedQuery = typeof value === 'string' && query ? JSON.stringify(query).slice(1, -1) : query;
  return <><span className={`json-scalar ${value === null ? 'null' : typeof value}`}><JsonSearchText text={long && !expanded ? jsonTextPreview(text, escapedQuery, 500) : text} query={escapedQuery}/></span>{long ? <button type="button" className="json-expand" onClick={() => setExpanded(current => !current)}>{expanded ? 'Show less' : 'Show full value'}</button> : null}</>;
}

function TreeNode({ value, name, path, focus, focusKey, query }: { value: unknown; name: string; path: JsonPath; focus: JsonPath | null; focusKey: string; query: string }) {
  const [open, setOpen] = useState(path.length < 2), [start, setStart] = useState(0), [interacting, setInteracting] = useState(false);
  const row = useRef<HTMLElement>(null);
  const object = value !== null && typeof value === 'object';
  const keys = useMemo(() => object ? Object.keys(value) : [], [value, object]);
  const selected = !!focus && focus.length === path.length && pathStartsWith(focus, path);
  const descendant = !!focus && focus.length > path.length && pathStartsWith(focus, path);
  const childIndex = descendant ? keys.indexOf(String(focus![path.length])) : -1;
  useLayoutEffect(() => { if (start >= keys.length && start !== 0) setStart(0); }, [start, keys.length]);
  useEffect(() => {
    if (selected || descendant) setOpen(true);
    if (descendant && childIndex >= 0) setStart(fieldWindow(childIndex));
  }, [focusKey, selected, descendant, childIndex]);
  useLayoutEffect(() => { if (selected) row.current?.scrollIntoView({ block: 'center', inline: 'nearest' }); }, [selected, focusKey]);
  const left = Math.min(path.length, 12) * 12;
  const actions = path.length > 0 && interacting ? <FieldActions value={value} path={path} /> : null;
  const events = { onPointerEnter: () => setInteracting(true), onPointerLeave: (event: PointerEvent<HTMLElement>) => { if (!event.currentTarget.contains(document.activeElement)) setInteracting(false); }, onFocus: () => setInteracting(true), onBlur: (event: FocusEvent<HTMLElement>) => { if (!event.currentTarget.contains(event.relatedTarget)) setInteracting(false); } };
  if (!object) return <div {...events} ref={element => { row.current = element; }} tabIndex={0} className={`json-tree-row ${selected?'json-field-selected':''}`} data-json-path={jsonPath(path)} style={{ paddingLeft: left }}><span className="json-key"><JsonSearchText text={name} query={query}/></span><Scalar value={value} query={query} />{actions}</div>;
  const end = Math.min(start + 50, keys.length);
  const page = (next: number) => { setStart(next); row.current?.focus({ preventScroll: true }); row.current?.scrollIntoView({ block: 'start', inline: 'nearest' }); };
  return <details className="json-tree-node" open={open} onToggle={event => { if (event.target === event.currentTarget) setOpen(event.currentTarget.open); }}>
    <summary {...events} ref={element => { row.current = element; }} className={selected?'json-field-selected':''} data-json-path={jsonPath(path)} style={{ paddingLeft: left }}><span className="json-key"><JsonSearchText text={name} query={query}/></span><span className="json-summary">{Array.isArray(value)?'Array':'Object'}({keys.length})</span>{actions}</summary>
    {open ? keys.length ? <>
      {start > 0 ? <div className="json-field-page" style={{ marginLeft: left+12 }}><span>Fields {start+1}–{end} of {keys.length}</span><button type="button" onClick={() => page(0)}>First fields</button><button type="button" onClick={() => page(Math.max(0,start-50))}>Previous 50</button></div> : null}
      {keys.slice(start,end).map(key=><TreeNode key={key} name={key} value={(value as Record<string,unknown>)[key]} path={[...path,Array.isArray(value)?Number(key):key]} focus={focus} focusKey={focusKey} query={query}/>)}
      {end < keys.length ? <button type="button" className="json-expand" style={{ marginLeft:left+12 }} onClick={() => page(start+50)}>Next {Math.min(50,keys.length-end)} fields · {end} of {keys.length}</button> : null}
    </> : <div className="json-tree-row" style={{ paddingLeft:left+12 }}><span className="json-summary">empty</span></div> : null}
  </details>;
}

export function JsonValue({ value, label = 'root' }: { value: unknown; label?: string }) {
  const { feedback, copy, dismiss } = useJsonClipboard(value);
  const [searchOpen,setSearchOpen]=useState(false),[query,setQuery]=useState(''),[mode,setMode]=useState<'matches'|'tree'>('matches');
  const [result,setResult]=useState<{value:unknown;query:string;matches:JsonMatch[];truncated:boolean;error?:string}|null>(null);
  const [selection,setSelection]=useState<{value:unknown;path:JsonPath;index:number}|null>(null);
  const [visible,setVisible]=useState(100);
  const input=useRef<HTMLInputElement>(null),searchButton=useRef<HTMLButtonElement>(null);
  const resultsId=useId();
  const needle=searchOpen?query.trim():'';
  const current=result && result.value===value && result.query===needle?result:null;
  const matches=current?.matches??[];
  const searching=!!needle&&!current;
  const focus=selection && selection.value===value?selection.path:null;
  const focusKey=JSON.stringify([focus,mode]);
  const resultsVisible=!!needle&&mode==='matches';
  useEffect(()=>{setResult(null);setSelection(null);},[value]);
  useEffect(()=>{
    if(!needle){setResult(null);return;}
    const abort=new AbortController();
    const timer=setTimeout(()=>{
      void searchJson(value,needle,abort.signal).then(found=>{if(!abort.signal.aborted)setResult({value,query:needle,...found});}).catch(error=>{if(!abort.signal.aborted)setResult({value,query:needle,matches:[],truncated:false,error:error instanceof Error?error.message:'Could not search JSON.'});});
    },200);
    return()=>{clearTimeout(timer);abort.abort();};
  },[value,needle]);
  useEffect(()=>{if(searchOpen)input.current?.focus({preventScroll:true});},[searchOpen]);
  const reveal=(index:number)=>{const match=matches[index];if(match){setSelection({value,path:match.path,index});setMode('tree');}};
  const closeSearch=()=>{setSearchOpen(false);setQuery('');setSelection(null);setResult(null);searchButton.current?.focus({preventScroll:true});};
  const download=()=>{const url=URL.createObjectURL(new Blob([JSON.stringify(value,null,2)],{type:'application/json'}));const link=document.createElement('a');link.href=url;link.download=`${label}.json`;link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);};
  return <div className="json-tree-shell">
    <div className="json-actions"><ToolButton ref={searchButton} label="Search JSON" aria-expanded={searchOpen} onClick={()=>searchOpen?closeSearch():setSearchOpen(true)}><SearchValueIcon/></ToolButton><ToolButton label={feedback?.key==='root'?feedback.failed?'Copy failed':'Copied JSON':'Copy JSON'} onClick={event=>void copy(event.currentTarget,'root',value)}>{feedback?.key==='root'&&!feedback.failed?<CheckIcon/>:<CopyIcon/>}</ToolButton><ToolButton label="Download JSON" onClick={download}><DownloadIcon/></ToolButton></div>
    {feedback?.manualText !== undefined && <ManualCopyText label="JSON to copy" text={feedback.manualText} onDismiss={dismiss} />}
    {searchOpen?<div className="json-search"><div className="json-search-input"><SearchValueIcon/><input ref={input} type="search" aria-label="Search JSON fields and values" aria-controls={resultsVisible?resultsId:undefined} placeholder="Search fields and values…" value={query} onChange={event=>{setQuery(event.target.value);setMode('matches');setSelection(null);setVisible(100);}} onKeyDown={event=>{if(event.key==='Escape'){event.preventDefault();event.stopPropagation();closeSearch();}else if(event.key==='Enter'&&matches.length){event.preventDefault();const index=selection && selection.value===value?selection.index:(event.shiftKey?0:-1);reveal((index+(event.shiftKey?-1:1)+matches.length)%matches.length);}}}/>{searching?<LoadingIndicator small decorative label="Searching JSON"/>:null}</div><div className="json-search-status"><span role="status">{searching?'Searching…':current?.error??(needle?`${matches.length}${current?.truncated?'+':''} ${matches.length===1&&!current?.truncated?'match':'matches'}`:'Search the loaded JSON')}</span>{needle&&mode==='tree'?<button type="button" onClick={()=>setMode('matches')}>Back to results</button>:null}<button type="button" onClick={closeSearch}>Close search</button></div></div>:null}
    {resultsVisible?<div id={resultsId} className="json-search-results" aria-label="JSON search results" aria-busy={searching}>{matches.slice(0,visible).map((match,index)=><div className="json-search-result" key={jsonPath(match.path)}><button type="button" className="json-match-open" aria-label={`Show ${jsonPath(match.path)} in JSON`} onClick={()=>reveal(index)}><code><JsonSearchText text={jsonPath(match.path)} query={needle}/></code><span><JsonSearchText text={jsonPreview(match.value,160,needle)} query={needle}/></span></button><FieldActions value={match.value} path={match.path}/></div>)}{!searching&&!current?.error&&!matches.length?<p>No matching fields or values.</p>:null}{matches.length>visible?<button type="button" className="json-expand" onClick={()=>setVisible(count=>count+100)}>More matches</button>:null}{current?.truncated?<p>Showing the first 1,000 matches. Refine your search for more specific results.</p>:null}</div>:null}
    <div className="json-tree" hidden={resultsVisible} inert={resultsVisible}><TreeNode value={value} name={label} path={[]} focus={focus} focusKey={focusKey} query={needle}/></div>
    <span className="sr-only" role="status">{feedback && feedback.manualText === undefined ? feedback.failed?'Copy failed. Select the content and copy it.':'Copied JSON':''}</span>
  </div>;
}

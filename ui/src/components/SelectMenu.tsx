import { useEffect, useId, useLayoutEffect, useRef, useState, type KeyboardEvent } from 'react';
import { createPortal } from 'react-dom';
export interface SelectOption { value: string; label: string; description?: string; disabled?: boolean }
export function SelectMenu({ label, value, options, onChange, placeholder = 'Choose…', disabled = false, searchable = false, onSearch, loading = false, error, onRetry, hasMore = false, onMore, className = '', onOpenChange }: {
  label: string; value: string; options: SelectOption[]; onChange: (value: string) => void; placeholder?: string;
  disabled?: boolean; searchable?: boolean; onSearch?: (value: string) => void; loading?: boolean; error?: string; onRetry?: () => void;
  hasMore?: boolean; onMore?: () => void; className?: string; onOpenChange?: (open: boolean) => void;
}) {
  const [open, setOpen] = useState(false), [text, setText] = useState(''), [active, setActive] = useState(0);
  const [position, setPosition] = useState({ left: 0, top: 0, width: 0, height: 260, above: false });
  const root = useRef<HTMLDivElement>(null), menu = useRef<HTMLDivElement>(null), input = useRef<HTMLInputElement>(null), button = useRef<HTMLButtonElement>(null);
  const remembered = useRef<SelectOption | undefined>(undefined);
  const current = options.find(option => option.value === value);
  useLayoutEffect(() => { if (current) remembered.current = current; }, [current]);
  const id = useId(), selected = current ?? (remembered.current?.value === value ? remembered.current : undefined);
  useEffect(() => { onOpenChange?.(open); }, [open, onOpenChange]);
  const visible = searchable && !onSearch && text ? options.filter(option => option.label.toLowerCase().includes(text.toLowerCase())) : options;
  const focus = () => (searchable ? input.current : button.current)?.focus({ preventScroll: true });
  const show = () => { if (disabled || open) return; setText(''); onSearch?.(''); setActive(Math.max(0, options.findIndex(option => option.value === value))); setOpen(true); };
  const close = () => { setOpen(false); focus(); };
  const pick = (option: SelectOption) => { if (option.disabled || loading) return; remembered.current = option; onChange(option.value); close(); };
  useEffect(() => { if (disabled) setOpen(false); }, [disabled]);
  useLayoutEffect(() => {
    if (!open || !root.current) return;
    const update = () => {
      const rect = root.current!.getBoundingClientRect();
      const below = window.innerHeight - rect.bottom - 10, above = rect.top - 10;
      const upward = below < 160 && above > below;
      const height = Math.min(280, Math.max(72, upward ? above : below));
      setPosition({ left: Math.max(8, Math.min(rect.left, window.innerWidth - rect.width - 8)), top: upward ? rect.top - 5 : rect.bottom + 5, width: rect.width, height, above: upward });
    };
    update(); const observer = new ResizeObserver(update); observer.observe(root.current);
    window.addEventListener('resize', update); window.addEventListener('scroll', update, true);
    return () => { observer.disconnect(); window.removeEventListener('resize', update); window.removeEventListener('scroll', update, true); };
  }, [open]);
  useEffect(() => {
    if (!open) return;
    const dismiss = (event: PointerEvent) => { const node = event.target as Node; if (!root.current?.contains(node) && !menu.current?.contains(node)) setOpen(false); };
    document.addEventListener('pointerdown', dismiss);
    return () => document.removeEventListener('pointerdown', dismiss);
  }, [open]);
  useEffect(() => { setActive(0); }, [text]);
  useEffect(() => { if (open) menu.current?.querySelector(`[data-option-index="${active}"]`)?.scrollIntoView({ block: 'nearest' }); }, [active, open]);
  const keyboard = (event: KeyboardEvent) => {
    if (event.key === 'Escape' && open) { event.preventDefault(); event.stopPropagation(); close(); }
    if (event.key === 'Tab') setOpen(false);
    if (['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key) && (open || event.key.startsWith('Arrow'))) {
      if (searchable && !event.key.startsWith('Arrow')) return;
      event.preventDefault();
      if (!open) { show(); return; }
      setActive(current => event.key === 'Home' ? 0 : event.key === 'End' ? visible.length - 1 : Math.max(0, Math.min(visible.length - 1, current + (event.key === 'ArrowDown' ? 1 : -1))));
    }
    if (event.key === 'Enter') { event.preventDefault(); if (!open) show(); else if (visible[active]) pick(visible[active]); }
  };
  const activeId = visible[active] ? `${id}-option-${active}` : undefined;
  return <div ref={root} className={`select-control ${className} ${open ? 'is-open' : ''}`} onKeyDown={keyboard}>
    {searchable ? <input ref={input} role="combobox" aria-label={label} aria-expanded={open} aria-controls={open ? id : undefined} aria-autocomplete="list" aria-activedescendant={open ? activeId : undefined} disabled={disabled} value={open ? text : selected?.label ?? value} placeholder={open ? 'Type to search…' : placeholder} autoComplete="off" maxLength={512} onFocus={show} onClick={show} onChange={event => { setText(event.target.value); onSearch?.(event.target.value); }} /> : <button ref={button} type="button" className="select-trigger" role="combobox" aria-label={label} aria-haspopup="listbox" aria-expanded={open} aria-controls={open ? id : undefined} aria-activedescendant={open ? activeId : undefined} disabled={disabled} title={selected?.label ?? value} onClick={() => open ? close() : show()}><span>{selected?.label ?? placeholder}</span></button>}
    <button type="button" className="select-chevron" aria-label={`Toggle ${label}`} tabIndex={-1} disabled={disabled} onMouseDown={event => event.preventDefault()} onClick={() => { if (open) close(); else { show(); focus(); } }}><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><path d="m4 6 4 4 4-4" /></svg></button>
    {open && root.current ? createPortal(<div ref={menu} className={`select-menu ${position.above ? 'opens-above' : ''}`} role="listbox" aria-label={label} id={id} style={{ left: position.left, top: position.top, width: position.width, maxHeight: position.height, transform: position.above ? 'translateY(-100%)' : undefined }} onScroll={event => { const area = event.currentTarget; if (hasMore && !loading && area.scrollHeight - area.scrollTop - area.clientHeight < 60) onMore?.(); }}>
      {loading ? <div className="select-feedback" role="status">Loading…</div> : null}{error ? <div className="select-feedback" role="alert">{error}{onRetry ? <button type="button" onClick={onRetry}>Retry</button> : null}</div> : null}
      {visible.map((option, index) => <button type="button" role="option" id={`${id}-option-${index}`} data-option-index={index} key={option.value} aria-selected={option.value === value} disabled={option.disabled || loading} tabIndex={-1} className={active === index ? 'is-active' : ''} onPointerDown={event => event.preventDefault()} onMouseMove={() => setActive(index)} onClick={() => pick(option)}><span><strong>{option.label}</strong>{option.description ? <small>{option.description}</small> : null}</span></button>)}
      {!visible.length && !loading && !error ? <div className="select-feedback">No matches</div> : null}{hasMore ? <button type="button" className="select-more" disabled={loading} onClick={onMore}>More results</button> : null}
    </div>, root.current.closest('dialog') ?? document.body) : null}
  </div>;
}

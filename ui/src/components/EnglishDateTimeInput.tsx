import { useId, useRef, useState } from 'react';
import { localDateKey, localDateTimeKey, parseLocalDateTime } from '../localDateTime';

const WEEKDAYS = ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'];
const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

export function EnglishDateTimeInput({ label, value, onChange }: { label: string; value: string; onChange: (value: string) => void }) {
  const id = useId();
  const trigger = useRef<HTMLButtonElement>(null);
  const [open, setOpen] = useState(false);
  const [month, setMonth] = useState(() => {
    const date = parseLocalDateTime(value) ?? new Date();
    date.setDate(1); date.setHours(12, 0, 0, 0);
    return date;
  });
  const valid = !value || Boolean(parseLocalDateTime(value));
  const today = localDateKey(new Date());
  const days = new Date(month.getFullYear(), month.getMonth() + 1, 0).getDate();
  const selected = value.slice(0, 10);
  const time = value.includes('T') ? value.split('T')[1] : '00:00:00';
  const timeParts = [24, 60, 60].map((limit, index) => {
    const part = Number(time.split(':')[index] ?? 0);
    return Number.isInteger(part) && part >= 0 && part < limit ? part : 0;
  });
  const selectTime = (index: number, part: string) => {
    const parts = timeParts.map((current, position) => String(position === index ? Number(part) : current).padStart(2, '0'));
    const date = parseLocalDateTime(`${selected}T00:00:00`) ? selected : today;
    onChange(`${date}T${parts.join(':')}`);
  };
  const selectDate = (date: string) => onChange(`${date}T${/^\d{2}:\d{2}(?::\d{2})?$/.test(time) ? time : '00:00:00'}`);
  const moveMonth = (delta: number) => setMonth(current => { const date = new Date(current); date.setMonth(date.getMonth() + delta); return date; });
  const close = () => { setOpen(false); trigger.current?.focus({ preventScroll: true }); };
  return <div className="english-date-time" lang="en" onKeyDown={event => { if (event.key === 'Escape' && open) { event.preventDefault(); event.stopPropagation(); close(); } }}>
    <div className="date-time-field"><label htmlFor={id}>{label}</label><div className="date-time-input"><input id={id} type="text" aria-label={`Invocation start ${label.toLowerCase()}`} aria-invalid={!valid} aria-describedby={!valid ? `${id}-error` : undefined} placeholder="YYYY-MM-DD HH:mm:ss" value={value.replace('T', ' ')} onChange={event => onChange(event.target.value.replace(' ', 'T'))} autoComplete="off" spellCheck={false} /><button ref={trigger} type="button" aria-label={`Choose ${label.toLowerCase()} date and time`} aria-expanded={open} aria-controls={open ? `${id}-calendar` : undefined} title="Choose date and time" onClick={() => {
      if (!open) { const date = parseLocalDateTime(value) ?? new Date(); date.setDate(1); date.setHours(12, 0, 0, 0); setMonth(date); }
      setOpen(current => !current);
    }}><svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.4" aria-hidden="true"><rect x="3" y="4" width="14" height="13" rx="2"/><path d="M6 2v4m8-4v4M3 8h14m-10 3h2m2 0h2m-6 3h2"/></svg></button></div></div>
    {!valid ? <p id={`${id}-error`} role="alert">Enter a valid date and time: YYYY-MM-DD HH:mm:ss.</p> : null}
    {open ? <section className="english-calendar" id={`${id}-calendar`} aria-label={`${label} date and time`}>
      <div className="calendar-heading"><button type="button" aria-label="Previous month" disabled={month.getFullYear() === 1 && month.getMonth() === 0} onClick={() => moveMonth(-1)}>‹</button><select aria-label={`${label} calendar month`} value={month.getMonth()} onChange={event => setMonth(current => { const next = new Date(current); next.setMonth(Number(event.target.value)); return next; })}>{MONTHS.map((name, index) => <option key={name} value={index}>{name}</option>)}</select><input key={month.getFullYear()} type="number" min="1" max="9999" aria-label={`${label} calendar year`} defaultValue={month.getFullYear()} onBlur={event => { const year = Number(event.target.value); if (Number.isInteger(year) && year >= 1 && year <= 9999) setMonth(current => { const next = new Date(current); next.setFullYear(year); return next; }); else event.target.value = String(month.getFullYear()); }} onKeyDown={event => { if (event.key === 'Enter') { event.preventDefault(); event.currentTarget.blur(); } }} /><button type="button" aria-label="Next month" disabled={month.getFullYear() === 9999 && month.getMonth() === 11} onClick={() => moveMonth(1)}>›</button></div>
      <div className="calendar-days">{WEEKDAYS.map(day => <span className="calendar-weekday" key={day}>{day}</span>)}{Array.from({ length: month.getDay() }, (_, index) => <span key={`blank-${index}`} />)}{Array.from({ length: days }, (_, index) => {
        const date = new Date(month); date.setDate(index + 1); const key = localDateKey(date);
        return <button type="button" key={key} aria-label={date.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' })} aria-pressed={selected === key} aria-current={key === today ? 'date' : undefined} className={selected === key ? 'selected' : ''} onClick={() => selectDate(key)}>{index + 1}</button>;
      })}</div>
      <div className="calendar-time"><span>Time</span><div className="calendar-time-selectors">{['Hour', 'Minute', 'Second'].map((unit, index) => <label key={unit}><span>{unit}</span><select aria-label={`${label} ${unit.toLowerCase()}`} value={timeParts[index]} onChange={event => selectTime(index, event.target.value)}>{Array.from({ length: index === 0 ? 24 : 60 }, (_, part) => <option key={part} value={part}>{String(part).padStart(2, '0')}</option>)}</select></label>)}</div></div>
      <div className="calendar-footer"><button type="button" onClick={() => { const now = new Date(); onChange(localDateTimeKey(now)); now.setDate(1); now.setHours(12, 0, 0, 0); setMonth(now); }}>Now</button><button type="button" onClick={() => { const date = new Date(); date.setDate(1); date.setHours(12, 0, 0, 0); setMonth(date); selectDate(today); }}>Today</button><button type="button" onClick={() => { onChange(''); close(); }}>Clear</button><button type="button" onClick={close}>Done</button></div>
    </section> : null}
  </div>;
}

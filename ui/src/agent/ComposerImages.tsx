import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { LoadingIndicator } from '../components/LoadingState.js';
import { Tooltip } from '../components/Tooltip.js';
import { AgentIcon } from './AgentIcon.js';

export const imageTypes = ['image/png', 'image/jpeg', 'image/gif', 'image/webp'];
export const maxImages = 4;
const mib = 1024 * 1024;

export function validateImages(files: File[]) {
  if (files.length > maxImages) return `Attach up to ${maxImages} images per message.`;
  if (files.some(file => !imageTypes.includes(file.type) || !file.size)) return 'Choose a PNG, JPEG, GIF, or WebP image.';
  if (files.some(file => file.size > 8 * mib)) return 'Each image must be 8 MiB or smaller.';
  if (files.reduce((total, file) => total + file.size, 0) > 16 * mib) return 'Images must total 16 MiB or smaller per message.';
}

export function AddImages({ mobile, preparing, disabled, onPick }: { mobile: boolean; preparing: boolean; disabled: boolean; onPick: (files: File[]) => void }) {
  const picker = useRef<HTMLInputElement>(null);
  return <>
    <input ref={picker} type="file" aria-label="Choose images" accept={mobile ? 'image/*' : imageTypes.join(',')} multiple hidden onChange={event => {
      const files = Array.from(event.currentTarget.files ?? []);
      event.currentTarget.value = '';
      if (files.length) onPick(files);
    }} />
    <Tooltip content="Add images" disabled={mobile}><button type="button" aria-label={preparing ? 'Preparing images' : 'Add images'}
      aria-busy={preparing || undefined} className="agent-add-images" disabled={disabled || preparing} onClick={() => picker.current?.click()}>
      {preparing ? <LoadingIndicator label="Preparing images" small decorative /> : <AgentIcon name="plus" />}
    </button></Tooltip>
  </>;
}

interface DraftImage { file: File; id: string }

function ImagePreview({ source, name, onClose }: { source: string; name: string; onClose: () => void }) {
  const dialog = useRef<HTMLDialogElement>(null);
  useLayoutEffect(() => {
    const element = dialog.current;
    element?.showModal();
    return () => element?.close();
  }, []);
  return <dialog ref={dialog} className="agent-image-preview" aria-label={name}
    onCancel={event => { event.preventDefault(); onClose(); }}
    onClick={event => { if (event.target === event.currentTarget) onClose(); }}>
    <img src={source} alt={name} />
    <button className="agent-image-preview-close" type="button" aria-label="Close image preview" onClick={onClose}><AgentIcon name="close" /></button>
  </dialog>;
}

function Thumbnail({ image, disabled, active, remove }: { image: DraftImage; disabled: boolean; active: boolean; remove: () => void }) {
  const [source, setSource] = useState('');
  const [preview, setPreview] = useState(false);
  useEffect(() => {
    const url = URL.createObjectURL(image.file);
    setSource(url);
    return () => URL.revokeObjectURL(url);
  }, [image.file]);
  useEffect(() => { if (!active) setPreview(false); }, [active]);
  return <div className="agent-image-thumb">
    <button type="button" className="agent-image-open" aria-label={`Preview ${image.file.name}`} aria-haspopup="dialog"
      disabled={!source} onClick={() => setPreview(true)}>{source ? <img src={source} alt="" /> : null}</button>
    <button type="button" className="agent-image-remove" aria-label={`Remove ${image.file.name}`} disabled={disabled}
      onClick={remove}><AgentIcon name="close" /></button>
    {active && preview && source && <ImagePreview source={source} name={image.file.name} onClose={() => setPreview(false)} />}
  </div>;
}

export function ImageThumbnails({ images, disabled, active, onRemove }: { images: DraftImage[]; disabled: boolean; active: boolean; onRemove: (id: string) => void }) {
  return <div className="agent-composer-images" role="group" aria-label="Attached images">
    {images.map(image => <Thumbnail key={image.id} image={image} active={active} disabled={disabled} remove={() => onRemove(image.id)} />)}
  </div>;
}

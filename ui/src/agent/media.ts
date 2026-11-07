export function safeUrl(value: unknown, image = false): string | undefined {
  if (typeof value !== 'string') return;
  if (image && /^data:image\/(png|jpeg|gif|webp);base64,[a-z\d+/=\s]+$/i.test(value)) return value;
  try { const url = new URL(value); if (url.protocol === 'https:' || url.protocol === 'http:') return url.href; } catch { /* Unsupported source. */ }
}

export function mediaUrl(source: Record<string, unknown> | undefined, type: string): string | undefined {
  if (source?.type !== 'data') return safeUrl(source?.url);
  const allowed = type === 'image' ? /^image\/(?:png|jpeg|gif|webp)$/ : type === 'audio' ? /^audio\/(?:mpeg|mp3|wav|x-wav|ogg|webm|mp4)$/ : type === 'video' ? /^video\/(?:mp4|webm|ogg)$/ : null;
  if (!allowed || typeof source.mime_type !== 'string' || !allowed.test(source.mime_type) || typeof source.data !== 'string' || !/^[a-z\d+/=]+$/i.test(source.data)) return;
  return `data:${source.mime_type};base64,${source.data}`;
}

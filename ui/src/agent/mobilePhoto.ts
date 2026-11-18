const heifType = /^image\/hei[cf](?:-sequence)?$/i;
const heifExtension = /\.hei[cf]$/i;

export function isHeifPhoto(file: File) {
  return heifType.test(file.type) || heifExtension.test(file.name);
}

/** Use Safari's image decoder and keep the existing JPEG upload contract. */
export async function prepareMobilePhoto(file: File): Promise<File> {
  if (!isHeifPhoto(file)) return file;
  if (file.size > 32 * 1024 * 1024) throw new Error('Choose an HEIC photo under 32 MiB.');

  const source = URL.createObjectURL(file.type ? file : file.slice(0, file.size, 'image/heic'));
  const image = new Image();
  const canvas = document.createElement('canvas');
  try {
    image.src = source;
    await image.decode();
    const scale = Math.min(1, 3072 / Math.max(image.naturalWidth, image.naturalHeight));
    canvas.width = Math.max(1, Math.round(image.naturalWidth * scale));
    canvas.height = Math.max(1, Math.round(image.naturalHeight * scale));
    const context = canvas.getContext('2d', { alpha: false });
    if (!context) throw new Error('Canvas is unavailable.');
    context.drawImage(image, 0, 0, canvas.width, canvas.height);
    const jpeg = await new Promise<Blob | null>(resolve => canvas.toBlob(resolve, 'image/jpeg', .9));
    if (jpeg?.type !== 'image/jpeg') throw new Error('JPEG conversion failed.');
    return new File([jpeg], file.name.replace(/\.[^.]+$/, '') + '.jpg', { type: jpeg.type });
  } catch {
    throw new Error(`Could not open ${file.name}. Choose a JPEG or PNG, or try in Safari on iOS 17 or later.`);
  } finally {
    URL.revokeObjectURL(source);
    canvas.width = canvas.height = 0;
  }
}

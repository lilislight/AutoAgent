type Control = HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement;

interface Constraints {
  validity: ValidityState;
  type?: string;
  min?: string;
  max?: string;
  minLength?: number;
  maxLength?: number;
}

/** Browser validation messages follow the browser UI locale, not html.lang. */
export function englishValidationMessage(field: Constraints): string {
  const value = field.validity;
  if (value.valid || value.customError) return '';
  if (value.badInput) return field.type === 'number' ? 'Please enter a number.' : 'Please enter a valid value.';
  if (value.valueMissing) {
    if (field.type === 'checkbox') return 'Please check this box to continue.';
    if (field.type === 'radio' || field.type?.startsWith('select')) return 'Please select an option.';
    return 'Please fill out this field.';
  }
  if (value.typeMismatch) return field.type === 'email' ? 'Please enter a valid email address.' : 'Please enter a valid URL.';
  if (value.patternMismatch) return 'Please match the requested format.';
  if (value.rangeUnderflow) return `Please enter a value greater than or equal to ${field.min}.`;
  if (value.rangeOverflow) return `Please enter a value less than or equal to ${field.max}.`;
  if (value.stepMismatch) return 'Please enter a value that matches the required step.';
  if (value.tooShort) return `Please use at least ${field.minLength} characters.`;
  if (value.tooLong) return `Please use no more than ${field.maxLength} characters.`;
  return 'Please enter a valid value.';
}

/** Keep native constraint checks and focus behavior, with English-only hints. */
export function installEnglishValidation(document: Document): () => void {
  const messages = new WeakMap<Control, string>();
  const control = (target: EventTarget | null): Control | null =>
    target instanceof Element && target.matches('input, select, textarea') ? target as Control : null;
  const clear = (field: Control) => {
    const previous = messages.get(field);
    if (previous !== undefined && field.validationMessage === previous) field.setCustomValidity('');
    messages.delete(field);
  };
  const invalid = (event: Event) => {
    const field = control(event.target);
    if (!field?.willValidate) return;
    clear(field);
    const message = englishValidationMessage(field);
    if (message) { field.setCustomValidity(message); messages.set(field, message); }
  };
  const edited = (event: Event) => {
    const field = control(event.target);
    if (!field) return;
    clear(field);
    if (field instanceof HTMLInputElement && field.type === 'radio' && field.name) {
      for (const element of field.form?.elements ?? document.getElementsByName(field.name)) {
        if (element instanceof HTMLInputElement && element.type === 'radio' && element.name === field.name && element.form === field.form) clear(element);
      }
    }
  };
  const reset = (event: Event) => {
    if (event.target instanceof HTMLFormElement) for (const element of event.target.elements) {
      const field = control(element); if (field) clear(field);
    }
  };
  document.addEventListener('invalid', invalid, true);
  document.addEventListener('input', edited, true);
  document.addEventListener('change', edited, true);
  document.addEventListener('reset', reset, true);
  return () => {
    document.removeEventListener('invalid', invalid, true);
    document.removeEventListener('input', edited, true);
    document.removeEventListener('change', edited, true);
    document.removeEventListener('reset', reset, true);
    for (const element of document.querySelectorAll('input, select, textarea')) {
      const field = control(element); if (field) clear(field);
    }
  };
}

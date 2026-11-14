import test from 'node:test';
import assert from 'node:assert/strict';
import { englishValidationMessage } from '../src/englishValidation.js';

const validity = (patch: Partial<ValidityState>): ValidityState => ({
  valid: false, valueMissing: false, typeMismatch: false, patternMismatch: false,
  rangeUnderflow: false, rangeOverflow: false, stepMismatch: false, tooShort: false,
  tooLong: false, badInput: false, customError: false, ...patch,
});

test('required controls have English messages regardless of the browser locale', () => {
  assert.equal(englishValidationMessage({ validity: validity({ valueMissing: true }) }), 'Please fill out this field.');
  assert.equal(englishValidationMessage({ type: 'checkbox', validity: validity({ valueMissing: true }) }), 'Please check this box to continue.');
  assert.equal(englishValidationMessage({ type: 'select-one', validity: validity({ valueMissing: true }) }), 'Please select an option.');
});

test('connection fields keep URL, number, pattern and range checks in English', () => {
  assert.equal(englishValidationMessage({ type: 'url', validity: validity({ typeMismatch: true }) }), 'Please enter a valid URL.');
  assert.equal(englishValidationMessage({ type: 'number', validity: validity({ badInput: true, valueMissing: true }) }), 'Please enter a number.');
  assert.equal(englishValidationMessage({ validity: validity({ patternMismatch: true }) }), 'Please match the requested format.');
  assert.equal(englishValidationMessage({ min: '0.1', validity: validity({ rangeUnderflow: true }) }), 'Please enter a value greater than or equal to 0.1.');
  assert.equal(englishValidationMessage({ max: '120', validity: validity({ rangeOverflow: true }) }), 'Please enter a value less than or equal to 120.');
});

test('valid fields and application-owned custom errors are not replaced', () => {
  assert.equal(englishValidationMessage({ validity: validity({ valid: true }) }), '');
  assert.equal(englishValidationMessage({ validity: validity({ customError: true }) }), '');
});

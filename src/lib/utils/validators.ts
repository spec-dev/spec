import { attemptToParseNumber } from './formatters'

export const isNonEmptyString = (val: any): boolean => typeof val === 'string' && val !== ''

export const couldBeColPath = (val: any): boolean => isNonEmptyString(val) && val.split('.').length === 3

// TODO: Tweak your approach to using this to include BigNumber.
export const couldBeNumber = (val: any): boolean => attemptToParseNumber(val) == val
export const isNonEmptyString = (val: any): boolean => typeof val === 'string' && val !== ''

export const couldBeColPath = (val: any): boolean => isNonEmptyString(val) && val.split('.').length === 3

export const couldBeNumber = (val: any): boolean => Number(val) == val
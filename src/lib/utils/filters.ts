import { FilterOp } from '../types'
import { couldBeNumber } from './validators'
import { toUTCDate } from './date'

export const filterOps = new Set(Object.values(FilterOp))

export const columnFilterOps = new Set([
    FilterOp.EqualTo,
    FilterOp.NotEqualTo,
    FilterOp.GreaterThan,
    FilterOp.GreaterThanOrEqualTo,
    FilterOp.LessThan,
    FilterOp.LessThanOrEqualTo,
])

export const multiValueFilterOps = new Set([
    FilterOp.In,
    FilterOp.NotIn,
])

export const numericFilterOps = new Set([
    FilterOp.GreaterThan,
    FilterOp.GreaterThanOrEqualTo,
    FilterOp.LessThan,
    FilterOp.LessThanOrEqualTo,
])

export const isColOperatorOp = (op: FilterOp): boolean => columnFilterOps.has(op) && op !== FilterOp.EqualTo

export function executeFilter(
    testValue: any, 
    op: FilterOp, 
    filterValue: any, 
    isDateTimeType: boolean = false,
): boolean {
    // Array type checks.
    if (multiValueFilterOps.has(op)) {
        if (!Array.isArray(filterValue) || !filterValue.length) {
            return false
        }

        // Check if the test value matches ANY of the filterValue[] entries.
        const foundMatch = !!filterValue.find(filterValueEntry => executeSingleValueFilter(
            testValue, 
            FilterOp.EqualTo, 
            filterValueEntry, 
            isDateTimeType,
        ))

        return op === FilterOp.In ? foundMatch : !foundMatch
    }

    return executeSingleValueFilter(testValue, op, filterValue, isDateTimeType)
}

function executeSingleValueFilter(
    testValue: any, 
    op: FilterOp, 
    filterValue: any, 
    isDateTimeType: boolean = false,
): boolean {
    // Date-time check.
    if (isDateTimeType) {
        // Convert both values to UTC dates.
        testValue = toUTCDate(testValue)
        filterValue = toUTCDate(filterValue)

        // Ensure both are valid dates.
        if (!testValue.isValid() || !filterValue.isValid()) {
            return false
        }
    }
    // Numeric comparison check.
    else if (numericFilterOps.has(op)) {
        if (!couldBeNumber(testValue) || !couldBeNumber(filterValue)) {
            return false
        }
        testValue = Number(testValue)
        filterValue = Number(filterValue)
    }

    switch (op) {
        case FilterOp.EqualTo:
            return testValue == filterValue

        case FilterOp.NotEqualTo:
            return testValue != filterValue

        case FilterOp.GreaterThan:
            return testValue > filterValue

        case FilterOp.GreaterThanOrEqualTo:
            return testValue >= filterValue

        case FilterOp.LessThan:
            return testValue < filterValue

        case FilterOp.LessThanOrEqualTo:
            return testValue <= filterValue
    
        default:
            return false
    }
}
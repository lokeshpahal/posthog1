import {
    AnyCohortCriteriaType,
    AnyCohortGroupType,
    BehavioralCohortType,
    BehavioralEventType,
    BehavioralLifecycleType,
    CohortCriteriaGroupFilter,
    CohortCriteriaType,
    CohortGroupType,
    CohortType,
    FilterLogicalOperator,
} from '~/types'
import { ENTITY_MATCH_TYPE, PROPERTY_MATCH_TYPE } from 'lib/constants'
import {
    BehavioralFilterKey,
    BehavioralFilterType,
    CohortClientErrors,
    FieldWithFieldKey,
} from 'scenes/cohorts/CohortFilters/types'
import { areObjectValuesEmpty, convertPropertyGroupToProperties, isNumeric } from 'lib/utils'
import { DeepPartialMap, ValidationErrorType } from 'kea-forms'
import equal from 'fast-deep-equal'
import { CRITERIA_VALIDATIONS, ROWS } from 'scenes/cohorts/CohortFilters/constants'

export function cleanBehavioralTypeCriteria(criteria: AnyCohortCriteriaType): AnyCohortCriteriaType {
    let type = undefined
    if (
        [
            BehavioralEventType.PerformEvent,
            BehavioralEventType.PerformMultipleEvents,
            BehavioralEventType.PerformSequenceEvents,
            BehavioralEventType.NotPerformSequenceEvents,
            BehavioralLifecycleType.PerformEventFirstTime,
            BehavioralLifecycleType.PerformEventRegularly,
            BehavioralLifecycleType.StopPerformEvent,
            BehavioralLifecycleType.StartPerformEventAgain,
        ].includes(criteria.value as BehavioralEventType | BehavioralLifecycleType)
    ) {
        type = BehavioralFilterKey.Behavioral
    }
    if (
        [BehavioralCohortType.InCohort, BehavioralCohortType.NotInCohort].includes(
            criteria.value as BehavioralCohortType
        )
    ) {
        type = BehavioralFilterKey.Cohort
    }
    if (
        [BehavioralEventType.HaveProperty, BehavioralEventType.NotHaveProperty].includes(
            criteria.value as BehavioralEventType
        )
    ) {
        type = BehavioralFilterKey.Person
    }
    return {
        ...criteria,
        ...(type ? { type } : {}),
    }
}

export function isCohortCriteriaGroup(
    criteria:
        | CohortCriteriaGroupFilter
        | AnyCohortCriteriaType[]
        | AnyCohortCriteriaType
        | AnyCohortGroupType[]
        | AnyCohortGroupType
        | undefined
): criteria is CohortCriteriaGroupFilter {
    if (criteria) {
        return (
            (criteria as CohortCriteriaGroupFilter).type !== undefined &&
            (criteria as CohortCriteriaGroupFilter).values !== undefined
        )
    }
    return false
}

export function isValidCohortGroup(criteria: AnyCohortGroupType): boolean {
    return !(
        (criteria.matchType === PROPERTY_MATCH_TYPE && !criteria.properties?.length) ||
        (criteria.matchType === ENTITY_MATCH_TYPE && !(criteria.action_id || criteria.event_id))
    )
}

export function createCohortFormData(cohort: CohortType, isNewCohortFilterEnabled: boolean = false): FormData {
    const rawCohort = {
        ...(cohort.name ? { name: cohort.name } : {}),
        ...(cohort.description ? { description: cohort.description } : {}),
        ...(cohort.csv ? { csv: cohort.csv } : {}),
        ...(cohort.is_static ? { is_static: cohort.is_static } : {}),
        ...(isNewCohortFilterEnabled
            ? {
                  filters: JSON.stringify(
                      cohort.is_static
                          ? {}
                          : /* Overwrite value with value_property for cases where value is not a behavior enum (i.e., cohort and person filters) */
                            {
                                properties: {
                                    ...applyAllCriteriaGroup(
                                        applyAllNestedCriteria(cohort, (criteriaList) =>
                                            criteriaList.map(
                                                (c) =>
                                                    ({
                                                        ...c,
                                                        ...('value_property' in c ? { value: c.value_property } : {}),
                                                        value_property: undefined,
                                                    } as AnyCohortCriteriaType)
                                            )
                                        ),
                                        (groupList) =>
                                            groupList.map((g) => ({
                                                ...g,
                                                id: undefined,
                                            }))
                                    ).filters.properties,
                                    id: undefined,
                                },
                            }
                  ),
                  groups: JSON.stringify([]),
              }
            : {
                  groups: JSON.stringify(
                      cohort.is_static
                          ? []
                          : cohort.groups.map((group: CohortGroupType) => ({
                                ...group,
                                id: undefined,
                                matchType: undefined,
                            }))
                  ),
              }),
    }

    if (!isNewCohortFilterEnabled) {
        delete rawCohort['filters']
    }

    // Must use FormData to encode file binary in request
    const cohortFormData = new FormData()
    for (const [itemKey, value] of Object.entries(rawCohort)) {
        cohortFormData.append(itemKey, value as string | Blob)
    }
    return cohortFormData
}

export function addLocalCohortGroupId(group: Partial<CohortGroupType>): CohortGroupType {
    const matchType = group.action_id || group.event_id ? ENTITY_MATCH_TYPE : PROPERTY_MATCH_TYPE

    return {
        matchType,
        id: Math.random().toString().substr(2, 5),
        ...group,
    }
}

export function processCohortOnSet(cohort: CohortType, isNewCohortFilterEnabled: boolean = false): CohortType {
    return {
        ...cohort,
        ...(isNewCohortFilterEnabled
            ? {
                  filters: {
                      /* Populate value_property with value and overwrite value with corresponding behavioral filter type */
                      properties: applyAllNestedCriteria(cohort, (criteriaList) =>
                          criteriaList.map((c) =>
                              c.type &&
                              [BehavioralFilterKey.Cohort, BehavioralFilterKey.Person].includes(c.type) &&
                              !('value_property' in c)
                                  ? {
                                        ...c,
                                        value_property: c.value,
                                        value:
                                            c.type === BehavioralFilterKey.Cohort
                                                ? BehavioralCohortType.InCohort
                                                : BehavioralEventType.HaveProperty,
                                    }
                                  : c
                          )
                      ).filters.properties,
                  },
              }
            : {
                  groups:
                      cohort.groups?.map((group) => ({
                          ...addLocalCohortGroupId(group),
                          ...(group.properties
                              ? { properties: convertPropertyGroupToProperties(group.properties) }
                              : {}),
                      })) ?? [],
              }),
    }
}

export function validateGroup(
    group: CohortCriteriaGroupFilter | AnyCohortCriteriaType
): DeepPartialMap<CohortCriteriaGroupFilter, ValidationErrorType> {
    if (!isCohortCriteriaGroup(group)) {
        return {}
    }

    // Validate at highest level first (groups and then criteria)
    const criteria = (group.values as AnyCohortCriteriaType[])
        .filter((g) => !isCohortCriteriaGroup(g))
        .map((c, index) => ({ ...c, index }))
    const negatedCriteria = criteria.filter((c) => !!c.negation)
    const negatedCriteriaIndices = new Set(negatedCriteria.map((c) => c.index))

    if (
        // Negation criteria can only be used when matching ALL criteria
        (group.type !== FilterLogicalOperator.And && negatedCriteria.length > 0) ||
        // Negation criteria has at least one positive matching criteria
        (group.type === FilterLogicalOperator.And && negatedCriteria.length === criteria.length)
    ) {
        return {
            id: CohortClientErrors.NegationCriteriaMissingOther,
            values: criteria.map((c) => ({
                value: negatedCriteriaIndices.has(c.index)
                    ? CohortClientErrors.NegationCriteriaMissingOther
                    : undefined,
            })) as DeepPartialMap<CohortCriteriaType, ValidationErrorType>[],
        }
    }

    // Negation where matching criteria cancel each other out
    const negatedFailingCriteriaIndices = new Set()
    negatedCriteria.forEach((negatedC) => {
        const baseCriteria = { ...negatedC }
        criteria.forEach((c) => {
            if (
                baseCriteria.index !== c.index &&
                equal(cleanCriteria(baseCriteria), Object.assign({}, cleanCriteria(c), { negation: !c.negation }))
            ) {
                negatedFailingCriteriaIndices.add(c.index)
                negatedFailingCriteriaIndices.add(baseCriteria.index)
            }
        })
    })

    if (
        group.type === FilterLogicalOperator.And &&
        negatedCriteria.length > 0 &&
        negatedFailingCriteriaIndices.size > 0
    ) {
        return {
            id: CohortClientErrors.NegationCriteriaCancel,
            values: criteria.map((c) => ({
                value: negatedFailingCriteriaIndices.has(c.index)
                    ? CohortClientErrors.NegationCriteriaCancel
                    : undefined,
            })) as DeepPartialMap<CohortCriteriaType, ValidationErrorType>[],
        }
    }

    // Complete event regularly time comparison
    const regularEventCriteria = criteria.filter((c) => c.value === BehavioralLifecycleType.PerformEventRegularly)
    if (
        regularEventCriteria.length > 0 &&
        regularEventCriteria.every((c) => isNumeric(c.min_periods) && typeof isNumeric(c.total_periods))
    ) {
        return {
            values: criteria.map((c) =>
                c.value === BehavioralLifecycleType.PerformEventRegularly
                    ? (c.min_periods ?? 0) > (c.total_periods ?? 0)
                        ? {
                              id: CohortClientErrors.RegularEventMismatch,
                              min_periods: CohortClientErrors.RegularEventMismatch,
                              total_periods: CohortClientErrors.RegularEventMismatch,
                          }
                        : {}
                    : {}
            ) as DeepPartialMap<CohortCriteriaType, ValidationErrorType>[],
        }
    }

    // Sequential times comparison
    const sequentialTimeCriteria = criteria.filter((c) =>
        [
            BehavioralLifecycleType.StartPerformEventAgain,
            BehavioralLifecycleType.StopPerformEvent,
            BehavioralEventType.PerformSequenceEvents,
        ].includes(c.value as BehavioralLifecycleType | BehavioralEventType)
    )
    if (
        sequentialTimeCriteria.length > 0 &&
        sequentialTimeCriteria.every((c) => isNumeric(c.seq_time_value) && isNumeric(c.time_value))
    ) {
        return {
            values: criteria.map((c) =>
                [
                    BehavioralLifecycleType.StartPerformEventAgain,
                    BehavioralLifecycleType.StopPerformEvent,
                    BehavioralEventType.PerformSequenceEvents,
                ].includes(c.value as BehavioralLifecycleType | BehavioralEventType)
                    ? (c.seq_time_value ?? 0) > (c.time_value ?? 0)
                        ? {
                              id: CohortClientErrors.SequentialTimeMismatch,
                              seq_time_value: CohortClientErrors.SequentialTimeMismatch,
                              time_value: CohortClientErrors.SequentialTimeMismatch,
                          }
                        : {}
                    : {}
            ) as DeepPartialMap<CohortCriteriaType, ValidationErrorType>[],
        }
    }

    // Generic criteria values cannot be empty
    return {
        values: criteria.map((c) => {
            const requiredFields = ROWS[criteriaToBehavioralFilterType(c)].fields.filter(
                (f) => !!f.fieldKey
            ) as FieldWithFieldKey[]

            const criteriaErrors = Object.fromEntries(
                requiredFields.map(({ fieldKey, type }) => [
                    fieldKey,
                    (
                        Array.isArray(c[fieldKey])
                            ? c[fieldKey].length > 0
                            : c[fieldKey] !== undefined && c[fieldKey] !== null && c[fieldKey] !== ''
                    )
                        ? undefined
                        : CRITERIA_VALIDATIONS?.[type](c[fieldKey]),
                ])
            )
            const consolidatedErrors = Object.values(criteriaErrors)
                .filter((e) => !!e)
                .join(' ')

            return {
                ...(areObjectValuesEmpty(criteriaErrors) ? {} : { id: consolidatedErrors }),
                ...criteriaErrors,
            }
        }),
    }
}

export function criteriaToBehavioralFilterType(criteria: AnyCohortCriteriaType): BehavioralFilterType {
    if (criteria.negation) {
        if (criteria.value === BehavioralEventType.PerformSequenceEvents) {
            return BehavioralEventType.NotPerformSequenceEvents
        }
        if (criteria.value === BehavioralEventType.PerformEvent) {
            return BehavioralEventType.NotPerformedEvent
        }
        if (criteria.type === BehavioralFilterKey.Person) {
            return BehavioralEventType.NotHaveProperty
        }
        if (criteria.type === BehavioralFilterKey.Cohort) {
            return BehavioralCohortType.NotInCohort
        }
    }
    return criteria.value ?? BehavioralEventType.PerformEvent
}

export function determineFilterType(
    type: BehavioralFilterKey,
    value: BehavioralFilterType,
    negation: boolean = false
): AnyCohortCriteriaType {
    if (
        value === BehavioralEventType.NotPerformSequenceEvents ||
        (value === BehavioralEventType.PerformSequenceEvents && negation)
    ) {
        return {
            type: BehavioralFilterKey.Behavioral,
            value: BehavioralEventType.PerformSequenceEvents,
            negation: true,
        }
    }
    if (value === BehavioralEventType.NotPerformedEvent || (value === BehavioralEventType.PerformEvent && negation)) {
        return {
            type: BehavioralFilterKey.Behavioral,
            value: BehavioralEventType.PerformEvent,
            negation: true,
        }
    }
    if (value === BehavioralCohortType.NotInCohort || (value === BehavioralCohortType.InCohort && negation)) {
        return {
            type: BehavioralFilterKey.Cohort,
            value: BehavioralCohortType.InCohort,
            negation: true,
        }
    }
    if (value === BehavioralEventType.NotHaveProperty || (value === BehavioralEventType.HaveProperty && negation)) {
        return {
            type: BehavioralFilterKey.Person,
            value: BehavioralEventType.HaveProperty,
            negation: true,
        }
    }

    return {
        type,
        value,
        negation: false,
    }
}

export function resolveCohortFieldValue(
    criteria: AnyCohortCriteriaType,
    fieldKey: string
): string | number | boolean | null | undefined {
    // Resolve correct behavioral filter type
    if (fieldKey === 'value') {
        return criteriaToBehavioralFilterType(criteria)
    }
    return criteria?.[fieldKey] ?? null
}

export function applyAllCriteriaGroup(
    oldCohort: CohortType,
    fn: (
        groupList: (AnyCohortCriteriaType | CohortCriteriaGroupFilter)[]
    ) => (AnyCohortCriteriaType | CohortCriteriaGroupFilter)[]
): CohortType {
    return {
        ...oldCohort,
        filters: {
            properties: {
                ...oldCohort.filters.properties,
                values: fn(oldCohort.filters.properties.values) as AnyCohortCriteriaType[],
            },
        },
    }
}

export function applyAllNestedCriteria(
    oldCohort: CohortType,
    fn: (criteriaList: AnyCohortCriteriaType[]) => (AnyCohortCriteriaType | CohortCriteriaGroupFilter)[],
    groupIndex?: number
): CohortType {
    return {
        ...oldCohort,
        filters: {
            properties: {
                ...oldCohort.filters.properties,
                values: oldCohort.filters.properties.values.map((group, groupI) =>
                    (groupIndex === undefined || groupI === groupIndex) && isCohortCriteriaGroup(group)
                        ? {
                              ...group,
                              values: fn(group.values as AnyCohortCriteriaType[]),
                          }
                        : group
                ) as CohortCriteriaGroupFilter[] | AnyCohortCriteriaType[],
            },
        },
    }
}

// Populate empty values with default values on changing type, pruning any extra variables
export function cleanCriteria(criteria: AnyCohortCriteriaType): AnyCohortCriteriaType {
    const populatedCriteria = {}
    const { fields, ...apiProps } = ROWS[criteriaToBehavioralFilterType(criteria)]
    Object.entries(apiProps).forEach(([key, defaultValue]) => {
        const nextValue = criteria[key] ?? defaultValue
        if (nextValue !== undefined && nextValue !== null) {
            populatedCriteria[key] = nextValue
        }
    })
    fields.forEach(({ fieldKey, defaultValue }) => {
        const nextValue = fieldKey ? criteria[fieldKey] ?? defaultValue : null
        if (fieldKey && nextValue !== undefined && nextValue !== null) {
            populatedCriteria[fieldKey] = nextValue
        }
    })
    return {
        ...populatedCriteria,
        ...determineFilterType(populatedCriteria['type'], populatedCriteria['value'], populatedCriteria['negation']),
    }
}

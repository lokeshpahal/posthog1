import { LemonSelectOptions } from 'lib/components/LemonSelect'
import {
    BehavioralCohortType,
    BehavioralEventType,
    BehavioralLifecycleType,
    PropertyFilterValue,
    PropertyOperator,
} from '~/types'
import { CohortFieldLogicProps } from 'scenes/cohorts/CohortFilters/cohortFieldLogic'
import { TaxonomicFilterGroupType } from 'lib/components/TaxonomicFilter/types'

export enum FilterType {
    Behavioral = 'behavioral',
    Aggregation = 'aggregation',
    Actors = 'actors',
    TimeUnit = 'timeUnit',
    DateOperator = 'dateOperator',
    MathOperator = 'mathOperator',
    EventsAndActionsMathOperator = 'eventsAndActionsMathOperator',
    Value = 'value',
    Text = 'text',
    EventsAndActions = 'eventsAndActions',
    EventProperties = 'eventProperties',
    PersonPropertyValues = 'personPropertyValues',
    EventType = 'eventType',
    Number = 'number',
    NumberTicker = 'numberTicker',
    CohortValues = 'cohortValues',
    CohortId = 'cohortId',
}

export enum FieldOptionsType {
    EventAggregation = 'eventAggregation',
    PropertyAggregation = 'propertyAggregation',
    Actors = 'actors',
    EventBehavioral = 'eventBehavioral',
    CohortBehavioral = 'cohortBehavioral',
    LifecycleBehavioral = 'lifecycleBehavioral',
    TimeUnits = 'timeUnits',
    DateOperators = 'dateOperators',
    MathOperators = 'mathOperators',
    ValueOptions = 'valueOptions',
    EventsAndActionsMathOperators = 'eventsAndActionsMathOperators',
}

export interface FieldValues {
    label: string
    values: LemonSelectOptions
    type: FieldOptionsType
}

export type BehavioralFilterType = BehavioralEventType | BehavioralCohortType | BehavioralLifecycleType

export enum BehavioralFilterKey {
    Behavioral = 'behavioral',
    Cohort = 'cohort',
    Person = 'person',
}

export interface Field {
    fieldKey?: string
    defaultValue?: string | number | null
    type: FilterType
    hide?: boolean // If field is hidden, key is included in final payload but no component is rendered
}

export interface FieldWithFieldKey extends Omit<Field, 'fieldKey'> {
    fieldKey: string
}

export interface Row {
    type: BehavioralFilterKey
    value?: BehavioralFilterType // Optional since some payloads override the value field
    fields: Field[]
    negation: boolean
}

// CohortField

export interface CohortFieldBaseProps extends Omit<CohortFieldLogicProps, 'cohortFilterLogicKey'> {
    cohortFilterLogicKey?: string
}

export interface CohortSelectorFieldProps extends CohortFieldBaseProps {
    placeholder?: string
}

export interface CohortTaxonomicFieldProps extends Omit<CohortFieldBaseProps, 'fieldOptionGroupTypes'> {
    placeholder?: string
    taxonomicGroupType?: TaxonomicFilterGroupType
    taxonomicGroupTypes?: TaxonomicFilterGroupType[]
    fieldOptionGroupTypes: never
}

export interface CohortPersonPropertiesValuesFieldProps extends Omit<CohortFieldBaseProps, 'fieldOptionGroupTypes'> {
    fieldOptionGroupTypes: never
    propertyKey: PropertyFilterValue | undefined
    operator?: PropertyOperator
}

export interface CohortTextFieldProps extends CohortFieldBaseProps {
    value: string
}

export interface CohortNumberFieldProps extends Omit<CohortFieldBaseProps, 'fieldOptionGroupTypes'> {
    fieldOptionGroupTypes: never
}

export type CohortFieldProps =
    | CohortSelectorFieldProps
    | CohortNumberFieldProps
    | CohortTaxonomicFieldProps
    | CohortTextFieldProps
    | CohortPersonPropertiesValuesFieldProps

export enum CohortClientErrors {
    NegationCriteriaMissingOther = 'Negation criteria are only supported after you have specified at least one positive matching criteria. Negation criteria can only be used when matching all criteria (AND).',
    NegationCriteriaCancel = 'These criteria cancel each other out, and would result in no matching persons.',
    RegularEventMismatch = 'The lowerbound period value must not be greater than the upperbound value.',
    EmptyEventsAndActions = 'Event or action cannot be empty.',
    EmptyEventProperties = 'Event property cannot be empty.',
    EmptyPersonPropertyValues = 'Person property value cannot be empty',
    EmptyEventType = 'Event type cannot be empty.',
    EmptyNumber = 'Period values must be at least 1 day and cannot be empty.',
    EmptyNumberTicker = 'Number cannot be empty.',
    EmptyTimeUnit = 'Time interval cannot be empty.',
    EmptyMathOperator = 'Math operator cannot be empty.',
    EmptyCohortId = 'Cohort id cannot be empty.',
    EmptyCohortValues = 'Cohort value cannot be empty.',
    EmptyValue = 'Event property value selector cannot be empty.',
    EmptyDateOperator = 'Date cannot be empty or invalid.',
    EmptyActors = 'Actors selector cannot be empty.',
    EmptyAggregation = 'Aggregation selector cannot be empty.',
    EmptyBehavioral = 'Behavioral selector cannot be empty.',
}

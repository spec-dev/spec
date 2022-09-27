import { SpecEvent } from '@spec.dev/event-client'

export type StringKeyMap = { [key: string]: any }

export type StringMap = { [key: string]: string }

export type AnyMap = { [key: string | number]: any }

export interface ColumnSourceConfig {
    object: string
    property: string
}

export interface ColumnConfig {
    source: ColumnSourceConfig | string
}

export type TableConfig = { [key: string]: ColumnConfig | string }

export type SchemaConfig = { [key: string]: TableConfig }

export type TablesConfig = { [key: string]: SchemaConfig }

export enum ColumnDefaultsSetOn {
    Insert = 'insert',
    Update = 'update',
}

export interface ColumnDefaultsConfig {
    value: string
    setOn?: ColumnDefaultsSetOn[]
}

export type TableDefaultsConfig = { [key: string]: ColumnDefaultsConfig | string }

export type SchemaDefaultsConfig = { [key: string]: TableDefaultsConfig }

export type DefaultsConfig = { [key: string]: SchemaDefaultsConfig }

export interface LiveObjectConfig {
    id: string
    filterBy?: StringKeyMap
    links: LiveObjectLink[]
}

export type LiveObjectsConfig = { [key: string]: LiveObjectConfig }

export interface ProjectConfig {
    objects?: LiveObjectsConfig
    tables?: TablesConfig
    defaults?: DefaultsConfig
}

export type MessageClientOptions = {
    hostname?: string
    port?: number
    onConnect?: () => void
}

export interface Event {
    name: string // i.e. "compound.CompoundMarketAPYUpdated@0.0.1"
}

export enum LiveObjectFunctionRole {
    GetOne = 'getOne',
    GetMany = 'getMany',
}

export interface EdgeFunction {
    name: string // i.e. "compound.marketAPY@0.0.1"
    args: { [key: string]: boolean }
    argsMap: StringMap
    metadata: StringKeyMap
    role: LiveObjectFunctionRole
    url: string
}

export interface LiveObjectLink {
    table: string
    inputs: StringMap
    seedWith: string[]
    uniqueBy?: string[]
    seedIfEmpty?: boolean
    eventsCanInsert?: boolean
    filterBy?: StringKeyMap
}

export interface LiveObject {
    id: string // i.e. "compound.CompoundMarketAPY@0.0.1"
    configName: string // i.e. "CompoundMarketAPY"
    filterBy?: StringKeyMap
    links: LiveObjectLink[]
    events: Event[]
    edgeFunctions: EdgeFunction[]
}

export interface ResolvedLiveObject {
    id: string // i.e. "compound.CompoundMarketAPY@0.0.1"
    events: Event[]
    edgeFunctions: EdgeFunction[]
}

export interface TableDataSource {
    columnName: string
}

export type TableDataSources = { [key: string]: TableDataSource[] }

export interface EventCursor {
    name: string
    id: string
    nonce: number
    timestamp: string | Date
}

export interface EventSub {
    name: string
    liveObjectIds: string[]
    cursor: EventCursor
    cursorChanged: boolean
    shouldBuffer: boolean
    buffer: SpecEvent<StringKeyMap | StringKeyMap[]>[]
}

export enum TableSubStatus {
    Pending,
    Creating,
    Subscribing,
    Subscribed,
}

export interface TableSub {
    schema: string
    table: string
    status: TableSubStatus
    primaryKeyTypes?: StringMap
    buffer: TableSubEvent[]
    processEvents: any
    blacklist: Set<string>
}

export interface TableSubEvent {
    timestamp: string
    operation: TriggerEvent
    schema: string
    table: string
    primaryKeys: StringKeyMap
    record?: StringKeyMap
    colNamesChanged?: string[]
    colNamesWithValues?: string[]
}

export interface TableSubCursor {
    tablePath: string
    timestamp: Date
}

export enum OpType {
    Insert = 'insert',
    Update = 'update',
    Delete = 'delete',
}

export interface Op {
    type: OpType
    schema: string
    table: string
    where?: StringKeyMap | StringKeyMap[]
    data?: StringKeyMap | StringKeyMap[]
    conflictTargets?: string[]
    liveTableColumns: string[]
    defaultColumnValues: { [key: string]: ColumnDefaultsConfig }
}

export enum SeedCursorStatus {
    InProgress = 'in-progress',
    Succeeded = 'succeeded',
    Failed = 'failed',
}

export enum SeedCursorJobType {
    SeedTable = 'seed-table',
    ResolveRecords = 'resolve-records',
}

export interface SeedCursor {
    id: string
    jobType: string
    spec: StringKeyMap
    status: SeedCursorStatus
    cursor: number
    metadata?: StringKeyMap | null
    createdAt: Date
}

export interface LiveColumn {
    columnPath: string
    liveProperty: string
}

export interface LiveColumnQuerySpec {
    columnPath: string
    liveProperty: string
}

export interface SeedSpec {
    liveObjectId: string
    tablePath: string
    linkProperties: StringMap
    seedWith: string[]
    uniqueBy: string[] | null
    filterBy: StringKeyMap | null
    seedColNames: string[]
    seedIfEmpty?: boolean
}

export interface ResolveRecordsSpec {
    liveObjectId: string
    tablePath: string
    primaryKeyData: StringKeyMap[]
}

export enum ConstraintType {
    ForeignKey = 'f',
    Unique = 'u',
    UniqueIndex = 'ui',
    PrimaryKey = 'p',
}

export interface Constraint {
    type: ConstraintType
    raw: string
    parsed: StringKeyMap
}

export interface ForeignKeyConstraint {
    schema: string
    table: string
    foreignSchema: string
    foreignTable: string
    foreignKey: string
    referenceKey: string
}

export enum TriggerEvent {
    INSERT = 'INSERT',
    UPDATE = 'UPDATE',
    MISSED = 'MISSED',
}

export interface Trigger {
    schema: string
    table: string
    event: TriggerEvent
    name: string
    joinedPrimaryKeys?: string
}

export interface DBColumn {
    name: string
    type: string
}

export interface TablesMeta {
    schema: string
    table: string
    primaryKey: DBColumn[]
    uniqueColGroups: string[][]
    foreignKeys: ForeignKeyConstraint[]
    colTypes: StringMap
}

export interface TableLink {
    liveObjectId: string
    link: LiveObjectLink
}

export interface TableLinkDataChanges {
    tableLink: TableLink
    events: TableSubEvent[]
}

export enum FilterOp {
    EqualTo = '=',
    GreaterThan = '>',
    GreaterThanOrEqualTo = '>=',
    LessThan = '>=',
    LessThanOrEqualTo = '<=',
}

export interface Filter {
    op: FilterOp
    column?: string
    value?: any
}

export interface Log {
    message: string
    level: LogLevel
    timestamp: string
}

export enum LogLevel {
    Info = 'info',
    Warn = 'warn',
    Error = 'error',
}
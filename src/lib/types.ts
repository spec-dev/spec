import { SpecEvent } from '@spec.dev/event-client'

export type StringKeyMap = { [key: string]: any }

export type StringMap = { [key: string]: string }

export type AnyMap = { [key: string | number]: any }

export enum FilterOp {
    EqualTo = '=',
    NotEqualTo = '!=',
    GreaterThan = '>',
    GreaterThanOrEqualTo = '>=',
    LessThan = '<',
    LessThanOrEqualTo = '<=',
    In = 'in',
    NotIn = 'not in',
}

export interface Filter {
    op: FilterOp
    column?: string
    value?: any
}

export type FilterGroup = { [key: string]: Filter }

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
    uniqueBy?: string[]
    filterBy?: FilterGroup[]
}

export interface LiveObject {
    id: string // i.e. "compound.CompoundMarketAPY@0.0.1"
    configName: string // i.e. "CompoundMarketAPY"
    links: LiveObjectLink[]
    events: Event[]
    edgeFunctions: EdgeFunction[]
    config: StringKeyMap
}

export interface ResolvedLiveObject {
    id: string // i.e. "compound.CompoundMarketAPY@0.0.1"
    events: Event[]
    edgeFunctions: EdgeFunction[]
    config: StringKeyMap
}

export interface TableDataSource {
    columnName: string
}

export type TableDataSources = { [key: string]: TableDataSource[] }

export interface EventCursor {
    name: string
    id: string
    nonce: string
    timestamp: string | Date
}

export interface EventSub {
    name: string
    liveObjectIds: string[]
    cursor: EventCursor
    cursorChanged: boolean
    lastNonceSeen: string
}

export interface ReorgEvent {
    id: string
    name: string
    chainId: string
    blockNumber: number
    eventTimestamp: string
}

export interface ReorgSub {
    chainId: string
    isProcessing: boolean
    buffer: ReorgEvent[]
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
    buffer: TableSubEvent[]
    processEvents: any
    blacklist: Set<string>
}

export interface TableSubEvent {
    timestamp: string
    operation: TriggerEvent
    schema: string
    table: string
    data?: StringKeyMap
    primaryKeyData?: StringKeyMap
    columnNamesChanged?: string[]
    nonEmptyColumns?: string[]
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
    primaryTimestampColumn: string | null
    defaultColumnValues: { [key: string]: ColumnDefaultsConfig }
}

export enum SeedCursorStatus {
    InLine = 'in-line',
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
    job_type: string
    spec: StringKeyMap
    status: SeedCursorStatus
    cursor: number
    metadata?: StringKeyMap | null
    createdAt: Date
}

export interface OpRecord {
    id: number
    table_path: string
    pk_names: string
    pk_values: string
    before: StringKeyMap | null
    after: StringKeyMap | null
    block_number: number
    chain_id: string
    ts: Date
}

export interface FrozenTable {
    id: number
    tablePath: string
    chainId: string
}

export interface LinksTableRecord {
    tablePath: string
    liveObjectId: string
    uniqueBy: string
    filterBy: string | null
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
    seedColNames: string[]
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
    foreignKey: string[]
    referenceKey: string[]
}

export enum TriggerProcedure {
    TableSub,
    TrackOps,
}

export enum TriggerEvent {
    INSERT = 'INSERT',
    UPDATE = 'UPDATE',
    DELETE = 'DELETE',
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
    enrichedLink: EnrichedLink
    filterColPaths?: string[]
}

export interface TableLinkDataChanges {
    tableLink: TableLink
    events: TableSubEvent[]
}

export interface Log {
    message: string
    level: LogLevel
    timestamp: string
    projectId: string
    env: string
}

export enum LogLevel {
    Info = 'info',
    Warn = 'warn',
    Error = 'error',
}

export interface EnrichedLink {
    liveObjectId: string
    tablePath: string
    uniqueByProperties: string[]
    uniqueConstraint: string[]
    linkOn: StringKeyMap
    filterBy: FilterGroup[]
}

export type SharedTablesQueryPayload = {
    sql: string
    bindings: any[]
}

export type MissedEventsCallback = (event: SpecEvent[]) => void

export enum OrderByDirection {
    ASC = 'asc',
    DESC = 'desc',
}

export type OrderBy = {
    column: string | string[]
    direction: OrderByDirection
}

export type SelectOptions = {
    orderBy?: OrderBy
    offset?: number
    limit?: number
    chainId?: string
    blockRange?: number[]
}

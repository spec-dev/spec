import { SpecEvent } from '@spec.dev/event-client'

export type StringKeyMap = { [key: string]: any }

export type StringMap = { [key: string]: string }

export type AnyMap = { [key: string | number]: any }

export interface ColumnSourceConfig {
    object: string
    property: string
}

export interface ColumnConfig {
    source: ColumnSourceConfig
}

export type TableConfig = { [key: string]: ColumnConfig }

export type SchemaConfig = { [key: string]: TableConfig }

export type TablesConfig = { [key: string]: SchemaConfig }

export interface LiveObjectConfig {
    id: string
    links: LiveObjectLink[]
}

export type LiveObjectsConfig = { [key: string]: LiveObjectConfig }

export interface ProjectConfig {
    project_id: string
    project_name: string
    objects?: LiveObjectsConfig
    tables?: TablesConfig
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
    args: { [key: string]: boolean },
    argsMap: StringMap,
    metadata: StringKeyMap,
    role: LiveObjectFunctionRole,
}

export interface LiveObjectLink {
    table: string
    properties: StringMap
    seedIfEmpty?: boolean
}

export interface LiveObject {
    id: string // i.e. "compound.CompoundMarketAPY@0.0.1"
    configName: string // i.e. "CompoundMarketAPY"
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
    timestamp: number
}

export interface EventSub {
    name: string
    liveObjectIds: string[]
    cursor: EventCursor
    cursorChanged: boolean
    shouldBuffer: boolean
    buffer: SpecEvent<StringKeyMap>[]
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
    where: StringKeyMap
    data?: StringKeyMap
}

export enum LiveColumnSeedStatus {
    InProgress = 'in-progress',
    Succeeded = 'succeeded',
    Failed = 'failed',
}

export interface LiveColumn {
    columnPath: string
    liveProperty: string
    seedStatus: LiveColumnSeedStatus
}

export interface LiveColumnQuerySpec {
    columnPath: string
    liveProperty: string
}

export interface SeedSpec {
    liveObjectId: string
    tablePath: string
    linkProperties: StringMap
    seedColNames: string[]
    seedIfEmpty?: boolean
}

export interface SpecFunctionResponse {
    data: any
    error: string | null
}

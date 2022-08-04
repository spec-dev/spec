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
    links: StringMap[]
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

export enum LiveObjectPropertyType {
    String = 'string',
    Number = 'number',
    Hash = 'hash',
}

export interface LiveObjectProperty {
    name: string
    type: LiveObjectPropertyType
}

export interface LiveObjectTypeDef {
    name: string
    properties: LiveObjectProperty[]
}

export interface Event {
    name: string // i.e. "compound.CompoundMarketAPYUpdated@0.0.1"
}

export interface EdgeFunction {
    name: string // i.e. "compound.marketAPY@0.0.1"
    url: string // i.e. "https://functions.spec.dev/compound.marketAPY@0.0.1"
}

export interface LiveObjectLink {
    table: string
    properties: StringMap
}

export interface LiveObject {
    id: string // i.e. "compound.CompoundMarketAPY@0.0.1"
    configName: string // i.e. "CompoundMarketAPY"
    links: LiveObjectLink[]
    // typeDef: LiveObjectTypeDef
    events: Event[]
    // edgeFunctions: EdgeFunction[]
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
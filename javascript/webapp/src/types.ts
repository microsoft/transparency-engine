/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */

export interface GraphData {
	nodes: GraphNode[]
	edges: GraphLink[]
}

export interface GraphNode {
	id: number
	name: string
	type: string
	flag: number
	x?: number
	y?: number
	relationship?: string
	encoding?: {
		shape?: string
		fill?: string
		stroke?: string
		strokeWidth?: number
		opacity?: number
		size?: number
	}
}

export interface GraphLink {
	source: number
	target: number
}

export interface Report {
	entityId: string
	reportSections: ReportSection[]
}

export interface ReportSection {
	title?: string
	intro?: string
	attribute_mapping: AttributeMapping
}

export interface AttributeMapping {
	attribute_counts?: AttributeBlock<TableAttribute>
	attribute_values?: AttributeBlock<TableAttribute | ComplexTableAttribute>
	attribute_charts?: AttributeBlock<ActivityAttribute>
}

export interface AttributeBlock<T> {
	title?: string
	intro?: string
	type?: ReportType
	format?: FormatType
	columns?: string[]
	data?: T
}

export type TableAttribute = [
	| AttributeCount
	| AttributeCountPercentRank
	| EntityValues[]
	| PrimitiveArray[]
	| Flag,
]

export type ComplexTableAttribute = EntityFlagsArray

export type AttributeCount = [string, number]
export type AttributeCountPercentRank = [string, number, number]

export type ArrayRow = PrimitiveArray | PrimitiveArray[]

export type PrimitiveArray = [string | number]

/**
 * This array documents a list of values for a given data type, one entry of entity + value per.
 */
export type EntityValues = {
	key: string
	value: EntityValue[]
}

export interface EntityValue {
	entity_id: string
	value: string | number
}

/**
 * This is a complex object for specifically documenting entity relationships and flags.
 */
export type EntityFlagsArray = FlagEntry[]

export type FlagEntry = [string, Paths, Flag[]]

export interface Paths {
	direct_paths: string[]
	indirect_paths: number
}

export interface Flag {
	flag: string
	evidence: Evidence[]
}

export type Evidence = Record<string, string | number>
/**
 * Describes the activities for an entity along with those of all related entities.
 * This therefore allows summarization across all related entities and intersection with the target entity.
 */
export type ActivityAttribute = [
	TargetEntityActivity,
	RelatedEntityActivityList,
]

/**
 * This is the list of activities by a single target entity.
 */
export interface TargetEntityActivity {
	key: string
	value: EntityActivity[]
}

/**
 * This is the activities by a set of related entities.
 * There should be one entry for each related entity,
 * and each of these will have a list of activities.
 */
export interface RelatedEntityActivityList {
	key: string
	value: RelatedEntityActivity[]
}

/**
 * This is the list of activities by a single related entity.
 */
export interface RelatedEntityActivity {
	entity_id: string
	activity: EntityActivity[]
}

/**
 * Basic activity definition for any entity.
 */
export interface EntityActivity {
	time: string
	attribute: string
	value: string
}

export enum FormatType {
	Table = 'table',
	ComplexTable = 'complex-table',
	Chart = 'chart',
}

export enum ReportType {
	CountPercentRank = 'count-percent-rank',
	Count = 'count',
	List = 'list',
	CountPercent = 'count-percent',
	Flags = 'flags',
}

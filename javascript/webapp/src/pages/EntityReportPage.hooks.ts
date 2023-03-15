/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import domToPdf from 'dom-to-pdf'
import { useCallback, useEffect, useMemo, useState } from 'react'

import {
	getEntityGraph,
	getEntityReport,
	getRelatedEntityGraph,
} from '../api/index.js'
import { useGraphData, useGraphDataMap } from '../hooks/useGraphData.js'
import type {
	AttributeBlock,
	ComplexTableAttribute,
	GraphData,
	Report,
} from '../types.js'
import { FormatType } from '../types.js'

/**
 * Executes service request to get the report for the given entity.
 * @param entityId
 * @returns
 */
export function useEntityReport(
	entityId: string | undefined,
): Report | undefined {
	const [report, setReport] = useState<Report | undefined>()

	useEffect(() => {
		const f = async () => {
			if (entityId) {
				const raw = await getEntityReport(entityId)
				setReport(raw)
			}
		}
		f()
	}, [entityId])

	return report
}

/**
 * Executes service request to get the graph for the given entity.
 * @param entityId
 * @returns
 */
export function useEntityGraph(
	entityId: string | undefined,
): GraphData | undefined {
	const [graph, setGraph] = useState<GraphData | undefined>()
	useEffect(() => {
		const f = async () => {
			if (entityId) {
				const raw = await getEntityGraph(entityId)
				setGraph(raw)
			}
		}
		f()
	}, [entityId])

	return useGraphData(graph)
}

/**
 * Executes service request to get related entity graphs for an entity.
 * It does this by interrogating the report to find all related entities,
 * and then invoking services to collect them.
 * @param entityId
 * @returns
 */
export function useRelatedEntityGraphs(
	report: Report | undefined,
): Map<string, GraphData> | undefined {
	const relatedIds = useRelatedEntityIds(report)
	const [graphs, setGraphs] = useState<Map<string, GraphData>>()
	useEffect(() => {
		const f = async () => {
			if (report?.entityId && relatedIds.length > 0) {
				const raw = new Map<string, GraphData>()
				await Promise.all(
					relatedIds.map(async (relatedId) => {
						const graph = await getRelatedEntityGraph(
							report.entityId,
							relatedId,
						)
						raw.set(relatedId, graph)
					}),
				)
				setGraphs(raw)
			}
		}
		f()
	}, [report, relatedIds])

	return useGraphDataMap(graphs)
}

function useRelatedEntityIds(report: Report | undefined): string[] {
	return useMemo(() => {
		const ids = new Set<string>()
		report?.reportSections.forEach(({ attribute_mapping }) => {
			if (
				attribute_mapping.attribute_values &&
				attribute_mapping.attribute_values.format === FormatType.ComplexTable
			) {
				;(
					attribute_mapping.attribute_values as AttributeBlock<ComplexTableAttribute>
				).data?.forEach(([id]) => {
					ids.add(id)
				})
			}
		})
		return Array.from(ids)
	}, [report])
}

export function useDownloadHandler(entityId: string | undefined) {
	const [generating, setGenerating] = useState<boolean>(false)
	const onDownloadRequested = useCallback(
		(e: React.MouseEvent<HTMLButtonElement | HTMLElement>) => {
			e.preventDefault()
			setGenerating(true)
			const f = async (id: string) => {
				pdfDownload(id, () => setGenerating(false))
			}
			entityId && f(entityId)
		},
		[entityId],
	)
	return {
		generating,
		onDownloadRequested,
	}
}

async function pdfDownload(
	entityId: string,
	onComplete?: () => void,
): Promise<void> {
	const element = document.querySelector('.report-content')
	const options = {
		filename: `report_${entityId}.pdf`,
		compression: 'MEDIUM',
		overrideWidth: 1200,
	}
	return domToPdf(element, options, onComplete) as Promise<void>
}

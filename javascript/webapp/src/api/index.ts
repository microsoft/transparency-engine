/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */

import type { GraphData, Report } from '../types'

// if any entity id has a "sample" prefix, that means we just want to load a test file from the local public folder

export async function getEntityGraph(entityId: string): Promise<GraphData> {
	const url = !entityId.toLowerCase().includes('sample')
		? `api/entities/graph/${entityId}`
		: `data/entity-graphs/${entityId}.json`
	return fetch(url).then((res) => res.json() as Promise<GraphData>)
}

export async function getRelatedEntityGraph(
	entityId: string,
	relatedId: string,
): Promise<GraphData> {
	const url = !entityId.toLowerCase().includes('sample')
		? `api/entities/graph/${entityId}?target=${relatedId}`
		: `data/related-entity-graphs/${entityId}-${relatedId}.json`
	return fetch(url).then((res) => res.json() as Promise<GraphData>)
}

export async function getEntityReport(entityId: string): Promise<Report> {
	const url = !entityId.toLowerCase().includes('sample')
		? `api/report/${entityId}`
		: `data/entity-reports/${entityId}.json`
	return fetch(url)
		.then(
			(res) => res.json() as Promise<{ html_report: Omit<Report, 'entityId'> }>,
		)
		.then((json) => json.html_report) // unwrap from the html_report property (TODO: remove this server-side)
		.then(
			(report) =>
				({
					entityId,
					...report,
				}) as Report,
		)
}

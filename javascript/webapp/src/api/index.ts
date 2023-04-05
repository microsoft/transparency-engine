/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */

import type { GraphData, Report } from '../types'

// if we're running locally in "test mode" with no api configured, we'll just pull in a sample json file for each call
/*eslint-disable @typescript-eslint/restrict-template-expressions*/
// TODO: get configured url from env once deployed, and remove the eslint comment
const API_URL = true

export async function getEntityGraph(entityId: string): Promise<GraphData> {
	const url = (API_URL && !entityId.toLowerCase().includes("sample"))
		? `api/entities/graph/${entityId}`
		: `data/entity-graphs/${entityId}.json`
	return fetch(url).then((res) => res.json() as Promise<GraphData>)
}

export async function getRelatedEntityGraph(
	entityId: string,
	relatedId: string,
): Promise<GraphData> {
	const url = (API_URL && !entityId.toLowerCase().includes("sample"))
		? `api/entities/graph/${entityId}?target=${relatedId}`
		: `data/related-entity-graphs/${entityId}.json`
	return fetch(url).then((res) => res.json() as Promise<GraphData>)
}

export async function getEntityReport(entityId: string): Promise<Report> {
	const url = (API_URL && !entityId.toLowerCase().includes("sample"))
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

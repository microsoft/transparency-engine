/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import type { Theme } from '@thematic/core'

import type { GraphData } from '../types'

/**
 * Takes a set of graph data and fills in the encoding block for each node based on desired viz options.
 * @param data
 * @param theme
 * @returns
 */
export function addGraphEncodings(data: GraphData, theme: Theme): GraphData {
	const colors = getCategoricalColors(data, theme)
	const warn = theme.application().error().hex()
	const normal = theme.application().background().hex()
	const bold = theme.rule().stroke().hex()
	return {
		nodes: data.nodes.map((node) => {
			return {
				...node,
				encoding: {
					shape: node.relationship === 'target' ? 'diamond' : 'circle',
					fill: colors.get(node.type),
					stroke:
						node.relationship === 'target'
							? bold
							: node.flag === 1
							? warn
							: normal,
					strokeWidth:
						node.flag === 1 || node.relationship === 'target' ? 3 : 1,
					opacity: 1.0,
					size:
						node.relationship === 'target'
							? 25
							: node.relationship === 'related'
							? 20
							: 10,
				},
			}
		}),
		// quick edge dedup. this may be done server-side in the future
		edges: Object.values(
			data.edges.reduce((acc, cur) => {
				const key = `${cur.source}-${cur.target}`
				acc[key] = cur
				return acc
			}, {}),
		),
	}
}

// only create the colors map once, so all graphs match.
// TODO: this could be in context instead of a module var for better control
const colors = new Map<string, string>()

function getCategoricalColors(data: GraphData, theme: Theme) {
	const types = new Set<string>(data?.nodes.map((node) => node.type))
	const nominal = theme.scales().nominal(10)
	// capture a couple of variants for the entity type
	colors.set('entity', theme.process().fill().hex())
	colors.set('EntityID', theme.process().fill().hex())
	Array.from(types).forEach((category, index) => {
		if (!colors.has(category)) {
			colors.set(category, nominal(index).hex())
		}
	})
	return colors
}

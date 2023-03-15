/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import { useThematic } from '@thematic/react'
import merge from 'lodash-es/merge.js'
import { memo, useMemo } from 'react'
import styled from 'styled-components'
import type { Spec } from 'vega'

import {
	parseJsonPathSpecMerged,
	VegaHost,
} from '../components/VegaHost/index.js'
import type { GraphData, GraphNode } from '../types.js'

export const VegaGraph: React.FC<{
	data: GraphData
	template: Spec
	width: number
	height: number
	onMouseClick?: (datum?: GraphNode, axis?: { x: number; y: number }) => void
}> = memo(function VegaGraph({ data, template, width, height, onMouseClick }) {
	const spec = useOverlay(data, template)
	const themed = useThemed(spec)
	return (
		<Container>
			<VegaHost
				spec={themed}
				width={width}
				height={height}
				onDatumClick={onMouseClick}
			/>
		</Container>
	)
})

function useOverlay(data: GraphData, template: Spec): Spec {
	return useMemo(() => {
		const nodeData = data.nodes.map((x, index) => ({ ...x, index }))
		const linkData = data.edges.map((x) => {
			const source = nodeData.find((a) => a.id === x.source)?.index
			const target = nodeData.find((a) => a.id === x.target)?.index
			return {
				source,
				target,
			}
		})

		const pathspec = {
			"$.data[?(@.name == 'node-data')].values": nodeData,
			"$.data[?(@.name == 'link-data')].values": linkData,
		}
		return parseJsonPathSpecMerged(template, pathspec)
	}, [data, template])
}

// add any theme defaults needed that may be missing. in this case, we want to map standard thematic graph links to the path mark used on the graph.
function useThemed(spec: Spec): Spec {
	const theme = useThematic()
	return useMemo(() => {
		const link = theme.link()
		return merge(
			{
				config: {
					path: {
						stroke: link.stroke().hex(),
						strokeWidth: link.strokeWidth(),
						strokeOpacity: link.strokeOpacity(),
					},
				},
			},
			spec,
		)
	}, [theme, spec])
}

const Container = styled.div`
	display: flex;
	flex-direction: column;
`

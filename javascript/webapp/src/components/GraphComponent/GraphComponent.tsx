/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */

import { memo } from 'react'
import styled from 'styled-components'
import type { Spec } from 'vega'

import { EntityId } from '../../styles/reports.js'
import type { GraphData } from '../../types.js'
import template from '../../vega/force-graph.json'
import { VegaGraph } from '../../vega/VegaGraph.js'

export const GraphComponent: React.FC<{
	data: GraphData
	width: number
	height: number
	title?: string
}> = memo(function GraphComponent({ data, width, height, title }) {
	return (
		<Container>
			{title && <EntityId>{title}</EntityId>}
			<VegaGraph
				template={template as Spec}
				data={data}
				width={width}
				height={height}
			/>
		</Container>
	)
})

const Container = styled.div``

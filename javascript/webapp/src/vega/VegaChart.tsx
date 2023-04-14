/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import { memo, useMemo } from 'react'
import styled from 'styled-components'
import type { Spec } from 'vega'

import {
	parseJsonPathSpecMerged,
	VegaHost,
} from '../components/VegaHost/index.js'

// this is intentional so the chart can flex to any data type
/* eslint-disable @typescript-eslint/no-explicit-any*/
export const VegaChart: React.FC<{
	data?: any[]
	template: Spec
	width: number
	height: number
	description: string
}> = memo(function VegaChart({ data, template, width, height, description }) {
	const spec = useOverlay(data, template)
	return (
		<Container>
			<VegaHost spec={spec} width={width} height={height} />
			<Description>{description}</Description>
		</Container>
	)
})

function useOverlay(data: any[] | undefined, template: Spec): Spec {
	return useMemo(() => {
		const pathspec = {
			"$.data[?(@.name == 'chart-data')].values": data,
		}
		return parseJsonPathSpecMerged(template, pathspec)
	}, [data, template])
}

const Container = styled.div`
	display: inline-block;
	flex-direction: column;
`

const Description = styled.div`
	font-size: 0.6em;
	padding-left: 5px;
	width: 95%;
	
`

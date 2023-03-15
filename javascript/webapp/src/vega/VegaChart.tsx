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
}> = memo(function VegaChart({ data, template, width, height }) {
	const spec = useOverlay(data, template)
	return (
		<Container>
			<VegaHost spec={spec} width={width} height={height} />
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

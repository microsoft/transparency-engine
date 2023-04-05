/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */

import { memo } from 'react'
import styled from 'styled-components'
import type { Spec } from 'vega'

import { useBarChartData } from '../../hooks/useBarChartData.js'
import { useDonutChartData } from '../../hooks/useDonutChartData.js'
import type {
	RelatedEntityActivity,
	TargetEntityActivity,
} from '../../types.js'
import barChartTemplate from '../../vega/bar-chart.json'
import donutChartTemplate from '../../vega/donut-chart.json'
import { VegaChart } from '../../vega/VegaChart.js'

export const SingleChartComponent: React.FC<{
	target: TargetEntityActivity
	related: RelatedEntityActivity
}> = memo(function SingleChartComponent({ target, related }) {
	//data manipulation
	const donutChartData = useDonutChartData(target, related)
	const barChartData = useBarChartData(target, related)

	return (
		<Container>
			<VegaChart
				template={donutChartTemplate as Spec}
				width={240}
				height={260}
				data={donutChartData}
			/>

			<VegaChart
				template={barChartTemplate as Spec}
				width={700}
				height={200}
				data={barChartData}
			/>
		</Container>
	)
})

const Container = styled.div`
	display: flex;
	align-items: flex-start;
	justify-content: space-between;
`

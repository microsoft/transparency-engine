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
				description="The donut chart summarizes the overall activity of the target entity and the related entitiy in terms of their distinct actions. It shows both the number of shared actions and the numbers of independent actions of each entity. The overall proportion of shared actions is a measure of the similarity of the two entities' activities over all time."
			/>

			<VegaChart
				template={barChartTemplate as Spec}
				width={700}
				height={200}
				data={barChartData}
				description="The column chart shows the number of shared and independent actions of each entity in each time period. If the overall activity similarity is high but there are relatively few shared actions in any given time period, then the entities have asynchronous activity similarity. If the entities consistently share a large number of actions in each time period, then they have synchronous activity similarity. "
			/>
		</Container>
	)
})

const Container = styled.div`
	display: flex;
	align-items: flex-start;
	justify-content: space-between;
`

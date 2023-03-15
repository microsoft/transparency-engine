/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */

import { memo } from 'react'
import styled from 'styled-components'

import { EntityId } from '../../styles/reports.js'
import type { ActivityAttribute, AttributeBlock } from '../../types.js'
import { SingleChartComponent } from '../SingleChartComponent/SingleChartComponent.js'

export const ChartComponent: React.FC<{
	dataObject: AttributeBlock<ActivityAttribute>
}> = memo(function ChartComponent({ dataObject }) {
	const target = dataObject.data?.[0]
	const related = dataObject.data?.[1]
	return (
		<Container>
			{dataObject.title && <EntityId>{dataObject.title}</EntityId>}
			{target &&
				related &&
				related.value.map((related, index) => (
					<SingleChartComponent
						target={target}
						related={related}
						key={`chart-${index}`}
					/>
				))}
		</Container>
	)
})

const Container = styled.div``

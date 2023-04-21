/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */

import { memo } from 'react'
import styled from 'styled-components'
import { If, Then, Else } from 'react-if'

import {
	IntroId,
	SubsectionContainer,
	SubsectionDescription,
	SubsectionTitle,
} from '../../styles/reports.js'
import {
	ActivityAttribute,
	AttributeBlock,
	ComplexMeasurementsTableAttribute,
	ComplexTableAttribute,
	GraphData,
	ReportType,
} from '../../types.js'
import { GraphComponent } from '../GraphComponent/GraphComponent.js'
import { AttributeValuesRelatedFlagsRow } from '../tables/AttributeValuesRelatedFlagsRow.js'
import { Table, Tbody } from '../tables/styles.js'
import { TableHead } from '../tables/TableHead.js'
import { AttributeValuesRelatedFlagsMeasurementsRow } from '../tables/AttributeValuesRelatedFlagsMeasurementsRow.js'
import { SingleChartComponent } from '../SingleChartComponent/SingleChartComponent.js'

export const ComplexTableComponent: React.FC<{
	dataObject: AttributeBlock<ComplexTableAttribute | ComplexMeasurementsTableAttribute>
	type: ReportType | undefined
	chartData: AttributeBlock<ActivityAttribute> | undefined 
	relatedGraphs: Map<string, GraphData> | undefined
}> = memo(function ComplexTableComponent({ dataObject, type, chartData, relatedGraphs }) {
	const target = chartData?.data?.[0]
	const related = chartData?.data?.[1]

	return (
		<SubsectionContainer>
			<SubsectionTitle>{dataObject.title}</SubsectionTitle>
			<SubsectionDescription>{dataObject.intro}</SubsectionDescription>

			<Tables>
				{dataObject.data?.map((row, ridx) => {
					const id = row[0] as string
					const graph = relatedGraphs?.get(id)
					const relatedActivity = related !== undefined ? related.value.filter(e => e.entity_id == id)[0] : undefined

					return (
						<TableGraphContainer key={`related-flags-${id}`}>

							{dataObject !== undefined && dataObject.columns !== undefined && (<IntroId>{dataObject?.columns[0]}:{row[0]}</IntroId>)}

							{graph && (
								<GraphComponent
									key={id}
									data={graph}
									height={450}
									width={450}
								/>
							)}

							<Table>
								{dataObject.columns && (
									<TableHead columns={dataObject.columns.slice(1)} />
								)}
								<Tbody>


								<If condition={type !== ReportType.FlagsMeasurements}>
									<Then>
										<AttributeValuesRelatedFlagsRow
											key={`row-${ridx}`}
											row={row}
										/>
									</Then>
									<Else>
										<AttributeValuesRelatedFlagsMeasurementsRow
											key={`row-${ridx}`}
											row={row}
										/>
									</Else>
								</If>
								</Tbody>
							</Table>

							{target &&
							related &&
							relatedActivity &&
							(<SingleChartComponent
								target={target}
								related={relatedActivity}
								key={`chart-1`}
							/>)}
						</TableGraphContainer>
					)
				})}
			</Tables>
		</SubsectionContainer>
	)
})

const Tables = styled.div`
	display: flex;
	flex-direction: column;
	gap: 24px;
`

const TableGraphContainer = styled.div`
	display: flex;
	flex-direction: column;
	gap: 10px;
`
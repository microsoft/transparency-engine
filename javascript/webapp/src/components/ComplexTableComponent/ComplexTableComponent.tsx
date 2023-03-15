/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */

import { memo } from 'react'
import styled from 'styled-components'

import {
	SubsectionContainer,
	SubsectionDescription,
	SubsectionTitle,
} from '../../styles/reports.js'
import type {
	AttributeBlock,
	ComplexTableAttribute,
	GraphData,
} from '../../types.js'
import { GraphComponent } from '../GraphComponent/GraphComponent.js'
import { AttributeValuesRelatedFlagsRow } from '../tables/AttributeValuesRelatedFlagsRow.js'
import { Table, Tbody } from '../tables/styles.js'
import { TableHead } from '../tables/TableHead.js'

export const ComplexTableComponent: React.FC<{
	dataObject: AttributeBlock<ComplexTableAttribute>
	relatedGraphs: Map<string, GraphData>
}> = memo(function ComplexTableComponent({ dataObject, relatedGraphs }) {
	return (
		<SubsectionContainer>
			<SubsectionTitle>{dataObject.title}</SubsectionTitle>
			<SubsectionDescription>{dataObject.intro}</SubsectionDescription>

			<Tables>
				{dataObject.data?.map((row, ridx) => {
					const id = row[0] as string
					const graph = relatedGraphs.get(id)
					return (
						<TableGraphContainer key={`related-flags-${id}`}>
							<Table>
								{dataObject.columns && (
									<TableHead columns={dataObject.columns} />
								)}
								<Tbody>
									<AttributeValuesRelatedFlagsRow
										key={`row-${ridx}`}
										row={row}
									/>
								</Tbody>
							</Table>
							{graph && (
								<GraphComponent
									key={id}
									data={graph}
									height={300}
									width={300}
								/>
							)}
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

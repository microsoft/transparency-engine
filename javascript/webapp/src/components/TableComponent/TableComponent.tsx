/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import { memo } from 'react'
import { Case, Switch } from 'react-if'

import {
	SubsectionContainer,
	SubsectionDescription,
	SubsectionTitle,
} from '../../styles/reports.js'
import type {
	AttributeBlock,
	AttributeCount,
	AttributeCountPercentRank,
	EntityValues,
	Flag,
	PrimitiveArray,
	TableAttribute,
} from '../../types.js'
import { ReportType } from '../../types.js'
import { AttributeCountsCountPercentRankRow } from '../tables/AttributeCountsCountPercentRankRow.js'
import { AttributeCountsCountPercentRow } from '../tables/AttributeCountsCountPercentRow.js'
import { AttributeCountsCountRow } from '../tables/AttributeCountsCountRow.js'
import { AttributeValuesListRow } from '../tables/AttributeValuesListRow.js'
import { AttributeValuesOwnFlagsRow } from '../tables/AttributeValuesOwnFlagsRow.js'
import { Table, Tbody } from '../tables/styles.js'
import { TableHead } from '../tables/TableHead.js'

export const TableComponent: React.FC<{
	dataObject: AttributeBlock<TableAttribute>
}> = memo(function TableComponent({ dataObject }) {
	const { type } = dataObject
	return (
		<SubsectionContainer>
			<SubsectionTitle>{dataObject.title}</SubsectionTitle>
			<SubsectionDescription>{dataObject.intro}</SubsectionDescription>
			<Table>
				{dataObject.columns && <TableHead columns={dataObject.columns} />}
				<Tbody>
					{dataObject.data?.map((row, ridx) => (
						<Row key={`row-${ridx}`} row={row} type={type} />
					))}
				</Tbody>
			</Table>
		</SubsectionContainer>
	)
})

const Row = ({ row, type }) => (
	<Switch>
		<Case condition={type === ReportType.Count}>
			<AttributeCountsCountRow row={row as AttributeCount} />
		</Case>
		<Case condition={type === ReportType.CountPercent}>
			<AttributeCountsCountPercentRow row={row as EntityValues} />
		</Case>
		<Case condition={type === ReportType.CountPercentRank}>
			<AttributeCountsCountPercentRankRow
				row={row as AttributeCountPercentRank}
			/>
		</Case>
		<Case condition={type === ReportType.List}>
			<AttributeValuesListRow row={row as PrimitiveArray[]} />
		</Case>
		<Case condition={type === ReportType.Flags}>
			<AttributeValuesOwnFlagsRow row={row as Flag} />
		</Case>
	</Switch>
)

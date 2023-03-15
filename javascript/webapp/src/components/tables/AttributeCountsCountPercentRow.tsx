/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import styled from 'styled-components'

import type { EntityValues } from '../../types.js'
import {
	Divider,
	FlexColumn,
	FlexRow,
	Key,
	Td,
	TextCell,
	Tr,
	Value,
} from './styles.js'

export interface AttributeCountsCountPercentRowProps {
	row: EntityValues
}

export const AttributeCountsCountPercentRow: React.FC<
	AttributeCountsCountPercentRowProps
> = ({ row }) => (
	<Tr>
		<TextCell>{row.key}</TextCell>
		<Td>
			<FlexColumn>
				{row.value.map((activity, index) => {
					return (
						<Pair key={`activity-value${index}`} style={{ fontSize: '0.9em' }}>
							<EntityRow>
								<Key>EntityId</Key> {activity.entity_id}
							</EntityRow>
							<Divider>|</Divider>
							<ValueRow>
								<Key>Value</Key>
								<Value>{activity.value}</Value>
							</ValueRow>
						</Pair>
					)
				})}
			</FlexColumn>
		</Td>
	</Tr>
)

const Pair = styled(FlexRow)`
	justify-content: space-between;
`

const EntityRow = styled(FlexRow)`
	min-width: 160px;
`

const ValueRow = styled(FlexRow)`
	width: 100px;
`

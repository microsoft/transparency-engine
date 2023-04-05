/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import styled from 'styled-components'

import type { PrimitiveArray } from '../../types.js'
import { FlexColumn, Td, TextCell, Tr } from './styles.js'

export interface AttributeValuedListRowProps {
	row: PrimitiveArray[]
}

export const AttributeValuesListRow: React.FC<AttributeValuedListRowProps> = ({
	row,
}) => (
	<Tr>
		<TextCell>{row[0]}</TextCell>
		<Td>
			<Rows>
				{row[1].map((value, index) => (
					<Row key={`list-cell${index}`}>{value}</Row>
				))}
			</Rows>
		</Td>
	</Tr>
)

const Rows = styled(FlexColumn)`
	gap: 8px;
`

const Row = styled.div``

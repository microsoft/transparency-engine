/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import type { AttributeCount } from '../../types.js'
import { NumberCell, TextCell, Tr } from './styles.js'

export interface AttributeCountsCountRowProps {
	row: AttributeCount
}

export const AttributeCountsCountRow: React.FC<AttributeCountsCountRowProps> =
	({ row }) => (
		<Tr>
			<TextCell>{row[0]}</TextCell>
			<NumberCell style={{ textAlign: 'right' }}>{row[1]}</NumberCell>
		</Tr>
	)

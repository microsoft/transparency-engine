/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */

import type { AttributeCountPercentRank } from '../../types.js'
import { NumberCell, TextCell, Tr } from './styles.js'

export interface AttributeCountsCountPercentRankRowProps {
	row: AttributeCountPercentRank
}

// TODO: the 100 - x math will be done in the service layer
export const AttributeCountsCountPercentRankRow: React.FC<
	AttributeCountsCountPercentRankRowProps
> = ({ row }) => (
	<Tr>
		<TextCell>{row[0]}</TextCell>
		<NumberCell>{row[1]}</NumberCell>
		<NumberCell>{`${row[2]}%`}</NumberCell>
	</Tr>
)

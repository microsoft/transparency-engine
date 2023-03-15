/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import styled from 'styled-components'

import type { Flag } from '../../types.js'
import { EvidenceCellContent } from './EvidenceCellContent.js'
import { Td, TextCell, Tr } from './styles.js'

export interface AttributeValuesOwnFlagsRowProps {
	row: Flag
}

export const AttributeValuesOwnFlagsRow: React.FC<
	AttributeValuesOwnFlagsRowProps
> = ({ row }) => (
	<Tr>
		<FlagCell>{row.flag}</FlagCell>
		<EvidenceCell>
			<EvidenceCellContent evidence={row.evidence} />
		</EvidenceCell>
	</Tr>
)

const FlagCell = styled(TextCell)`
	width: 50%;
`

const EvidenceCell = styled(Td)`
	width: 50%;
`

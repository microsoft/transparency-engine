/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import styled from 'styled-components'

import type { Evidence } from '../../types.js'
import { FlexColumn, FlexRow, Key, Value } from './styles.js'

export interface EvidenceCellContentProps {
	evidence: Evidence[]
}
export const EvidenceCellContent: React.FC<EvidenceCellContentProps> = ({
	evidence,
}) => (
	<Container>
		{evidence.map((evidence, index) => (
			<EvidenceRow key={`flag-cell${index}`}>
				{Object.entries(evidence).map(([key, value], eidx) => (
					<EvidenceEntry key={`evidence-entry-${eidx}`}>
						<EvidenceKey>{key}</EvidenceKey>
						<Value>{value}</Value>
					</EvidenceEntry>
				))}
			</EvidenceRow>
		))}
	</Container>
)

const Container = styled(FlexColumn)`
	gap: 20px;
`

const EvidenceRow = styled(FlexColumn)`
	gap: 4px;
`

const EvidenceEntry = styled(FlexRow)`
	font-size: 0.9em;
	justify-content: space-between;
`

const EvidenceKey = styled(Key)`
	min-width: 160px;
`

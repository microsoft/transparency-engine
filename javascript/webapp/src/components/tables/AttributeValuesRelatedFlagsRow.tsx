/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import styled from 'styled-components'

import type { Flag, FlagEntry, Paths } from '../../types.js'
import { EvidenceCellContent } from './EvidenceCellContent.js'
import { EntityIdCell, FlexColumn, Key, Td, Tr, Value } from './styles.js'

export interface AttributeValuesRelatedFlagsRowProps {
	row: FlagEntry
}

export const AttributeValuesRelatedFlagsRow: React.FC<
	AttributeValuesRelatedFlagsRowProps
> = ({ row }) => (
	<Tr>
		<EntityIdCell>{row[0]}</EntityIdCell>
		<PathsCell paths={row[1]} />
		<FlagCell flags={row[2]} />
	</Tr>
)

const PathsCell = ({ paths }: { paths: Paths }) => (
	<PathsContainer>
		<DirectPathsContainer>
			<Key>Direct paths</Key>
			<DirectPathsList>
				{paths.direct_paths.map((path, index) => (
					<DirectPathsValue key={`path-${index}`}>{path}</DirectPathsValue>
				))}
			</DirectPathsList>
		</DirectPathsContainer>
		<IndirectPathsContainer>
			<Key>Indirect paths</Key>
			<Value>{paths.indirect_paths}</Value>
		</IndirectPathsContainer>
	</PathsContainer>
)

const PathsContainer = styled(Td)``

const DirectPathsContainer = styled.div``
const DirectPathsList = styled.div`
	display: flex;
	flex-direction: column;
	gap: 8px;
	padding: 8px 8px 8px 20px;
	margin-bottom: 20px;
`
const DirectPathsValue = styled.div``

const IndirectPathsContainer = styled.div`
	display: flex;
	gap: 8px;
`

const FlagCell = ({ flags }: { flags: Flag[] }) => (
	<FlagsContainer>
		<FlagsColumn>
			{flags.map((flag, fidx) => (
				<FlagContainer key={`flag-${fidx}`}>
					<FlagName>{flag.flag}</FlagName>
					<EvidenceCellContent evidence={flag.evidence} />
				</FlagContainer>
			))}
		</FlagsColumn>
	</FlagsContainer>
)

const FlagsContainer = styled(Td)``

const FlagsColumn = styled(FlexColumn)`
	gap: 20px;
`

const FlagContainer = styled(FlexColumn)`
	gap: 8px;
`

const FlagName = styled.div``

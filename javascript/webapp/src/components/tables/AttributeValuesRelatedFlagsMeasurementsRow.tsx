/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import styled from 'styled-components'

import type { FlagMeasurement, FlagMeasurementEntry, Paths } from '../../types.js'
import { FlexColumn, Key, Td, Tr, Value, MeasureName } from './styles.js'

export interface AttributeValuesRelatedFlagsMeasurementsRowProps {
	row: FlagMeasurementEntry
}

export const AttributeValuesRelatedFlagsMeasurementsRow: React.FC<
AttributeValuesRelatedFlagsMeasurementsRowProps
> = ({ row }) => (
	<Tr>
		<PathsCell paths={row[1]} />
		<MeasurementsCell measurements={row[2]} />
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

const MeasurementsCell = ({ measurements }: { measurements: FlagMeasurement[] }) => (
	<FlagsContainer>
		<FlagsColumn>
			{measurements.map((measure, fidx) => (
				<MeasureContainer key={`flag-${fidx}`}>
					<MeasureName>{measure.key}</MeasureName>
					<MeasureValue>{measure.value}</MeasureValue>
				</MeasureContainer>
			))}
		</FlagsColumn>
	</FlagsContainer>
)

const FlagsContainer = styled(Td)``

const FlagsColumn = styled(FlexColumn)`
	gap: 20px;
`

const MeasureContainer = styled.div`
	gap: 8px;
	display: flex;
`

const MeasureValue = styled.div`
	display: inline-block;
`

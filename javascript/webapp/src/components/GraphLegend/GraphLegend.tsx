/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import type { ITheme } from '@fluentui/react'
import { useThematic } from '@thematic/react'
import { memo, useMemo } from 'react'
import styled from 'styled-components'

import type { GraphData } from '../../types.js'
export interface GraphLegendProps {
	data: GraphData
}

export const GraphLegend: React.FC<GraphLegendProps> = memo(
	function GraphLegend({ data }) {
		const types = useUniqueTypes(data)
		return (
			<Container>
				<Relationships />
				<Row>
					<Title>Attributes</Title>
					{types
						.filter((t) => t[0] !== 'EntityID' && t[0] !== 'entity')
						.map(([label, color]) => (
							<Type key={`legend-row-${label}`} label={label} color={color} />
						))}
				</Row>
			</Container>
		)
	},
)

const Type = ({ label, color }: { label: string; color?: string }) => {
	const theme = useThematic()
	const stroke = theme.application().background().hex()
	return (
		<Node>
			<Circle fill={color} stroke={stroke} size={11} />
			<Label>{label}</Label>
		</Node>
	)
}

const Relationships = () => {
	const theme = useThematic()
	const warn = theme.application().error().hex()
	const fill = theme.process().fill().hex()
	const stroke = theme.application().background().hex()
	const bold = theme.rule().stroke().hex()
	
	return (
		<Row>
			<Title>Entities</Title>
			<Node>
				<Diamond fill={fill} stroke={bold} strokeWidth={3} size={20} />
				<Label>target</Label>
			</Node>
			<Node>
				<Circle fill={fill} stroke={stroke} size={21} />
				<Label>related</Label>
			</Node>
			<Node>
				<Circle fill={fill} stroke={warn} strokeWidth={3} size={22} />
				<Label>flag</Label>
			</Node>
		</Row>
	)
}

const Container = styled.div`
	width: 100%;
	padding: 2px 8px;
	display: flex;
	justify-content: space-between;
`

const Title = styled.div`
	font-size: 0.7em;
	font-weight: bold;
	color: ${({ theme }: { theme: ITheme }) => theme.palette.neutralTertiary};
`

const Row = styled.div`
	display: flex;
	gap: 8px;
	align-items: center;
`

const Node = styled.div`
	display: flex;
	gap: 4px;
	align-items: center;
`

const Label = styled.div`
	font-size: 14px;
	color: ${({ theme }: { theme: ITheme }) => theme.palette.neutralSecondary};
`

const Diamond = styled.div<{
	size: number
	stroke: string
	fill: string
	strokeWidth?: number
}>`
	rotate: 45deg;
	width: ${({ size }) => size}px;
	height: ${({ size }) => size}px;
	border-width: ${({ strokeWidth }) => strokeWidth || 1}px;
	border-style: solid;
	border-color: ${({ stroke }) => stroke};
	background-color: ${({ fill }) => fill};
	margin-right: 2px;
`

const Circle = styled.div<{
	size: number
	stroke: string
	fill: string
	strokeWidth?: number
}>`
	width: ${({ size }) => size}px;
	height: ${({ size }) => size}px;
	border-radius: ${({ size }) => size}px;
	border-width: ${({ strokeWidth }) => strokeWidth || 1}px;
	border-style: solid;
	border-color: ${({ stroke }) => stroke};
	background-color: ${({ fill }) => fill};
`

// extract the unique type + color set from the data
function useUniqueTypes(data: GraphData) {
	return useMemo(() => {
		const unique = new Map<string, string | undefined>()
		data.nodes.forEach((node) => {
			unique.set(node.type, node.encoding?.fill)
		})
		return Array.from(unique)
	}, [data])
}

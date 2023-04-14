/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import type { ITheme } from '@fluentui/react'
import styled from 'styled-components'

export const Table = styled.table`
	border: 1px solid
		${({ theme }: { theme: ITheme }) => theme.palette.neutralQuaternary};
	border-collapse: collapse;
`

export const Thead = styled.thead`
	background-color: ${({ theme }: { theme: ITheme }) =>
		theme.palette.themePrimary};
	color: ${({ theme }: { theme: ITheme }) => theme.palette.white};
`

export const Tbody = styled.tbody``

export const Th = styled.th`
	background-color: ${({ theme }: { theme: ITheme }) =>
		theme.palette.themePrimary};
	padding: 6px 10px;
`

export const Tr = styled.tr`
	:nth-child(even) {
		background-color: ${({ theme }: { theme: ITheme }) =>
			theme.palette.neutralLighter};
	}
`

export const Td = styled.td`
	white-space: pre-wrap;
	border-bottom: 1px solid
		${({ theme }: { theme: ITheme }) => theme.palette.neutralQuaternaryAlt};
	padding: 6px 10px;
`

// semantic cell types for default formatting
export const TextCell = styled(Td)``
export const NumberCell = styled(Td)`
	text-align: right;
`
export const EntityIdCell = styled(TextCell)``

// useful content types for consistent visuals
export const Divider = styled.div`
	color: ${({ theme }: { theme: ITheme }) => theme.palette.neutralTertiary};
`

export const MeasureName = styled.div`
	font-weight: bold;
	color: ${({ theme }: { theme: ITheme }) => theme.palette.neutralSecondary};
	&:after {
		content: ':';
	}
`

export const Key = styled.div`
	font-weight: bold;
	color: ${({ theme }: { theme: ITheme }) => theme.palette.neutralSecondary};
	&:after {
		content: ':';
	}
`

export const Value = styled.div``

export const FlexRow = styled.div`
	display: flex;
	flex-direction: row;
	gap: 12px;
`

export const FlexColumn = styled.div`
	display: flex;
	flex-direction: column;
	gap: 12px;
`

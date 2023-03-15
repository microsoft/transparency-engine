/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import type { ITheme } from '@fluentui/react'
import styled from 'styled-components'

export const Container = styled.div`
	overflow: auto;
	width: 100%;
`

export const Header = styled.div`
	padding: 4px;
`

export const Download = styled.div`
	display: flex;
	gap: 12px;
	align-items: center;
`

export const Content = styled.div`
	display: flex;
	flex-direction: column;
	align-items: center;
`

export const Report = styled.div<{ width: number }>`
	display: flex;
	flex-direction: column;
	gap: 40px;
	width: ${({ width }) => width}px;
	font-size: 1.3em;
`

export const Intro = styled.div`
	display: flex;
	flex-direction: column;
	gap: 12px;
`
export const IntroTitle = styled.h1`
	color: ${({ theme }: { theme: ITheme }) => theme.palette.themePrimary};
	margin-bottom: 0;
`
export const IntroText = styled.div``
export const IntroId = styled.div`
	font-weight: bold;
	color: ${({ theme }: { theme: ITheme }) => theme.palette.neutralSecondary};
`

export const GraphContainer = styled.div<{ width: number; height: number }>`
	display: flex;
	flex-direction: column;
	width: ${({ width }) => width}px;
	height: ${({ height }) => height}px;
	justify-content: center;
	align-items: center;
`
export const Caption = styled.div`
	margin-top: 10px;
	font-style: italic;
	font-size: 0.7em;
`

/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import type { ITheme } from '@fluentui/react'
import styled from 'styled-components'

export const SectionContainer = styled.section``

export const SectionTitle = styled.h2`
	color: ${({ theme }: { theme: ITheme }) => theme.palette.themePrimary};
`

export const SectionDescription = styled.p``

export const SubsectionContainer = styled.div`
	margin-bottom: 30px;
`

export const SubsectionTitle = styled.h3`
	color: ${({ theme }: { theme: ITheme }) => theme.palette.neutralSecondaryAlt};
	margin-bottom: 0;
`

export const IntroId = styled.div`
	padding-top: 20px;
	font-weight: bold;
	color: ${({ theme }: { theme: ITheme }) => theme.palette.neutralSecondary};
`

export const SubsectionDescription = styled.p``

export const EntityId = styled.p`
	font-weight: bold;
	color: ${({ theme }: { theme: ITheme }) => theme.palette.neutralSecondary};
`

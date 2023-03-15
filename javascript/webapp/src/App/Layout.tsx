/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import { Spinner } from '@fluentui/react'
import type { PropsWithChildren } from 'react'
import React, { memo, Suspense } from 'react'

import { Header } from './Header.js'
import { Container, Content, Main } from './Layout.styles.js'

export const Layout: React.FC<PropsWithChildren> = memo(function Layout({
	children,
}) {
	return (
		<Container id="layout">
			<Header />
			<Main>
				<Suspense fallback={<Spinner />}>
					<Content>{children}</Content>
				</Suspense>
			</Main>
		</Container>
	)
})

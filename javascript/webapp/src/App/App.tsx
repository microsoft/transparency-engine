/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
/* eslint-disable  @typescript-eslint/ban-ts-comment, @typescript-eslint/no-unsafe-return */
import { BrowserRouter } from 'react-router-dom'
import { RecoilRoot } from 'recoil'

import { Layout } from './Layout.js'
import { Router } from './Router.js'
import { StyleContext } from './StyleContext.js'

export const App: React.FC = function App() {
	return (
		<StyleContext>
			<RecoilRoot>
				<BrowserRouter>
					<Layout>
						<Router />
					</Layout>
				</BrowserRouter>
			</RecoilRoot>
		</StyleContext>
	)
}

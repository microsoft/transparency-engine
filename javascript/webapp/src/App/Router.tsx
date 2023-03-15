/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
/* eslint-disable  @typescript-eslint/ban-ts-comment, @typescript-eslint/no-unsafe-return */
import { Route, Routes } from 'react-router-dom'

import { EntityReportPage } from '../pages/EntityReportPage.js'
import { HomePage } from '../pages/HomePage.js'

export const Router: React.FC = function Router() {
	return (
		<Routes>
			<Route path={'/'} element={<HomePage />} />
			<Route path={'/report/:entityId'} element={<EntityReportPage />} />
		</Routes>
	)
}

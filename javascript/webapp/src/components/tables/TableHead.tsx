/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import { Th, Thead, Tr } from './styles.js'

export interface TableHeadProps {
	columns: string[]
}

export const TableHead: React.FC<TableHeadProps> = ({ columns }) => (
	<Thead>
		<Tr>
			{columns?.map((col) => (
				<Th key={col}>{col}</Th>
			))}
		</Tr>
	</Thead>
)

/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import { useThematic } from '@thematic/react'
import { useMemo } from 'react'

import type { GraphData } from '../types.js'
import { addGraphEncodings } from '../utils/graphs.js'
/**
 * Takes the original service data and parses it to make sure it is ready for our graphs.
 * @param original
 */
export function useGraphData(
	data: GraphData | undefined,
): GraphData | undefined {
	const theme = useThematic()
	return useMemo(() => {
		if (data) {
			return addGraphEncodings(data, theme)
		}
	}, [theme, data])
}

export function useGraphDataMap(
	data: Map<string, GraphData> | undefined,
): Map<string, GraphData> | undefined {
	const theme = useThematic()
	return useMemo(() => {
		if (data) {
			const transformed = new Map<string, GraphData>()
			data.forEach((value, key) => {
				transformed.set(key, addGraphEncodings(value, theme))
			})
			return transformed
		}
		return data
	}, [theme, data])
}

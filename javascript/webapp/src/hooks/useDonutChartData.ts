/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import { useMemo } from 'react'

import type { RelatedEntityActivity, TargetEntityActivity } from '../types.js'

export interface QuarterlyActivitySummary {
	id: string
	field: number
}

/**
 * Takes the original service data and parses it to make sure it is ready for our charts.
 * @param target
 * @param related
 */
export function useDonutChartData(
	target: TargetEntityActivity,
	related: RelatedEntityActivity,
): QuarterlyActivitySummary[] {
	return useMemo(() => {
		let targetCounter = 0
		let relatedCounter = 0
		let sharedCounter = 0

		target.value.forEach((targetElement) => {
			//means is target only
			if (
				related.activity.find((e) => e.value === targetElement.value) ===
				undefined
			) {
				targetCounter++
			}
		})

		related.activity.forEach((relatedElement) => {
			//means is related only
			if (
				target.value.find((e) => e.value === relatedElement.value) === undefined
			) {
				relatedCounter++
			}
		})

		sharedCounter = target.value.length - targetCounter

		return [
			{ id: 'selected_only', field: targetCounter },
			{ id: 'related_only', field: relatedCounter },
			{ id: 'shared', field: sharedCounter },
		]
	}, [target, related])
}

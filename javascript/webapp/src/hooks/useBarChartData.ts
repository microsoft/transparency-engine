/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import { useMemo } from 'react'

import type { RelatedEntityActivity, TargetEntityActivity } from '../types.js'

export interface QuarterlyActivityEntry {
	position: number
	value: number
	quarter: string
}

/**
 * Takes the original service data and parses it to make sure it is ready for our charts.
 * @param target
 * @param related
 */
export function useBarChartData(
	target: TargetEntityActivity,
	related: RelatedEntityActivity,
): QuarterlyActivityEntry[] {
	return useMemo(() => {
		const result: QuarterlyActivityEntry[] = []

		const quarters = new Set<string>()

		target.value.forEach((targetElement) => {
			quarters.add(targetElement.time)
		})

		related.activity.forEach((relatedElement) => {
			quarters.add(relatedElement.time)
		})

		for (const quarter of quarters) {
			let targetCounter = 0
			let relatedCounter = 0
			let sharedCounter = 0

			target.value.forEach((targetElement) => {
				//means is target only
				if (targetElement.time === quarter) {
					if (
						related.activity.find(
							(e) => e.value === targetElement.value && e.time === quarter,
						) === undefined
					) {
						targetCounter++
					}
				}
			})

			related.activity.forEach((relatedElement) => {
				//means is related only
				if (relatedElement.time === quarter) {
					if (
						target.value.find(
							(e) => e.value === relatedElement.value && e.time === quarter,
						) === undefined
					) {
						relatedCounter++
					}
				}
			})

			if (target.value.filter((e) => e.time === quarter).length !== undefined) {
				const resultNumber = target.value.filter(
					(e) => e.time === quarter,
				).length

				sharedCounter = resultNumber - targetCounter
			} else {
				sharedCounter = 0
			}

			result.push(
				{ position: 0, value: targetCounter, quarter: quarter },
				{ position: 1, value: relatedCounter, quarter: quarter },
				{ position: 2, value: sharedCounter, quarter: quarter },
			)
		}

		return result
	}, [target, related])
}

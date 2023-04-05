/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import { useMemo } from 'react'

import type { RelatedEntityActivity, TargetEntityActivity } from '../types.js'

export interface TimedActivityEntry {
	position: number
	value: number
	time: string
}

/**
 * Takes the original service data and parses it to make sure it is ready for our charts.
 * @param target
 * @param related
 */
export function useBarChartData(
	target: TargetEntityActivity,
	related: RelatedEntityActivity,
): TimedActivityEntry[] {
	return useMemo(() => {
		const result: TimedActivityEntry[] = []

		const times = new Set<string>()

		target.value.forEach((targetElement) => {
			times.add(targetElement.time)
		})

		related.activity.forEach((relatedElement) => {
			times.add(relatedElement.time)
		})

		for (const time of times) {
			let targetCounter = 0
			let relatedCounter = 0
			let sharedCounter = 0

			target.value.forEach((targetElement) => {
				//means is target only
				if (targetElement.time === time) {
					if (
						related.activity.find(
							(e) => e.value === targetElement.value && e.time === time,
						) === undefined
					) {
						targetCounter++
					}
				}
			})

			related.activity.forEach((relatedElement) => {
				//means is related only
				if (relatedElement.time === time) {
					if (
						target.value.find(
							(e) => e.value === relatedElement.value && e.time === time,
						) === undefined
					) {
						relatedCounter++
					}
				}
			})

			if (target.value.filter((e) => e.time === time).length !== undefined) {
				const resultNumber = target.value.filter((e) => e.time === time).length

				sharedCounter = resultNumber - targetCounter
			} else {
				sharedCounter = 0
			}

			result.push(
				{ position: 0, value: targetCounter, time },
				{ position: 1, value: relatedCounter, time },
				{ position: 2, value: sharedCounter, time },
			)
		}

		return result.sort((a, b) => a.time.localeCompare(b.time))
	}, [target, related])
}

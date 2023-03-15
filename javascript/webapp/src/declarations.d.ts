/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */

declare module 'dom-to-pdf' {
	export interface PdfOptions {
		filename: string
		compression?: string
		overrideWidth?: number
	}
	export type Handler = () => void

	const pdf: (
		element: Element | null,
		options: PdfOptions,
		onComplete?: Handler,
	) => Promise<void>

	export default pdf
}

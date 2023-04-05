/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */

import { memo } from 'react'
import styled from 'styled-components'

import type { GraphData, Report } from '../../types.js'
import { ReportTemplateSection } from './ReportTemplateSection.js'

export interface ReportTemplateProps {
	report: Report
	relatedGraphs: Map<string, GraphData> | undefined
}

export const ReportTemplate: React.FC<ReportTemplateProps> = memo(
	function ReportTemplate({ report, relatedGraphs }) {
		return (
			<Container id="table">
				{report?.reportSections.map((section) => (
					<ReportTemplateSection
						key={section.title}
						section={section}
						relatedGraphs={relatedGraphs}
					/>
				))}
			</Container>
		)
	},
)

const Container = styled.div`
	display: flex;
	flex-direction: column;
	gap: 20px;
`

/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import { DefaultButton, Spinner } from '@fluentui/react'
import { memo } from 'react'
import { Else, If, Then, When } from 'react-if'
import { useParams } from 'react-router-dom'

import { GraphComponent } from '../components/GraphComponent/GraphComponent.js'
import { GraphLegend } from '../components/GraphLegend/GraphLegend.js'
import { ReportTemplate } from '../components/ReportTemplateComponent/index.js'
import {
	useDownloadHandler,
	useEntityGraph,
	useEntityReport,
	useRelatedEntityGraphs,
} from './EntityReportPage.hooks.js'
import {
	Caption,
	Container,
	Content,
	Download,
	GraphContainer,
	Header,
	Intro,
	IntroId,
	IntroText,
	IntroTitle,
	Report,
} from './EntityReportPage.styles.js'

const REPORT_WIDTH = 1000
const GRAPH_HEIGHT = 500

// eslint can't pick up the logical assertions in the When conditions
/*eslint-disable @typescript-eslint/no-non-null-assertion*/
export const EntityReportPage: React.FC = memo(function EntityReportPage() {
	const { entityId } = useParams()
	const report = useEntityReport(entityId)
	const graph = useEntityGraph(entityId)
	const relatedGraphs = useRelatedEntityGraphs(report)

	const { generating, onDownloadRequested } = useDownloadHandler(entityId)

	return (
		<Container>
			<Header>
				<Download>
					<DefaultButton
						disabled={!(graph || report) || generating}
						iconProps={{ iconName: 'PDF' }}
						onClick={onDownloadRequested}
					>
						Download as pdf
					</DefaultButton>
					<When condition={generating}>
						<Spinner
							label="Creating report PDF..."
							ariaLive="assertive"
							labelPosition="right"
						/>
					</When>
				</Download>
			</Header>
			<Content className="report-content">
				<Report width={REPORT_WIDTH}>
					<Intro>
						<IntroTitle>ENTITY REPORT</IntroTitle>
						<IntroText>
							This Transparency Engine report illustrates how risks or
							opportunities may transfer to an entity of interest via its
							network of closely related entities. Close relationships between
							pairs of entities are determined through statistical analysis of
							shared attributes and activity, while review flags (&ldquo;red
							flag&rdquo; risks or &ldquo;green flag&rdquo; opportunities) are
							provided as data inputs.
						</IntroText>
						<IntroId>Target Entity ID: {entityId}</IntroId>
					</Intro>
					<GraphContainer width={REPORT_WIDTH} height={GRAPH_HEIGHT + 25}>
						<If condition={graph !== undefined}>
							<Then>
								<GraphComponent
									data={graph!}
									width={REPORT_WIDTH}
									height={GRAPH_HEIGHT}
								/>
								<GraphLegend data={graph!} />
								<Caption>
									Network diagram showing how the target entity is connected to
									other closely-related entities via its attributes and
									activity.
								</Caption>
							</Then>
							<Else>
								<Spinner
									label="Loading entity graph..."
									ariaLive="assertive"
									labelPosition="right"
								/>
							</Else>
						</If>
					</GraphContainer>
					<If condition={report !== undefined && relatedGraphs !== undefined}>
						<Then>
							<ReportTemplate report={report!} relatedGraphs={relatedGraphs!} />
						</Then>
						<Else>
							<Spinner
								label="Loading entity report..."
								ariaLive="assertive"
								labelPosition="right"
							/>
						</Else>
					</If>
				</Report>
			</Content>
		</Container>
	)
})

/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import { PrimaryButton, TextField } from '@fluentui/react'
import { memo, useCallback, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import styled from 'styled-components'

export const HomePage: React.FC = memo(function HomePage() {
	const navigate = useNavigate()
	const [id, setId] = useState<string>('sample-1234')
	const handleChange = useCallback((_e, value) => setId(value), [])
	const handleClick = useCallback(() => {
		navigate(`/report/${id}`)
	}, [navigate, id])
	return (
		<Container>
			<p>
				It looks like you were trying to find an entity report, but didn&apos;t
				supply an entity ID. Please enter one in the textbox below.
			</p>

			<Inputs>
				<TextField placeholder="sample-1234" onChange={handleChange} />
				<PrimaryButton onClick={handleClick}>Load</PrimaryButton>
			</Inputs>
		</Container>
	)
})

const Container = styled.div`
	overflow: auto;
	width: 100%;
	padding: 20px;
`

const Inputs = styled.div`
	width: 280px;
	display: flex;
	gap: 8px;
`

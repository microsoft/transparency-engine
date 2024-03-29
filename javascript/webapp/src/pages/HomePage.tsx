/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
import { PrimaryButton, TextField } from '@fluentui/react'
import { memo, useCallback, useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import styled from 'styled-components'

export const HomePage: React.FC = memo(function HomePage() {
	const navigate = useNavigate()
	const [id, setId] = useState<string>('sample-220')
	const handleChange = useCallback((_e, value) => setId(value), [])
	const handleClick = useCallback(() => {
		navigate(`/report/${id}`)
	}, [navigate, id])
	const handleKey = useCallback((e) => {
		e.keyCode === 13 && handleClick()
	}, [handleClick])
	const ref = useRef<any>()
	useEffect(() => {
		// HACK: TextField doesn't expose regular ref properly, so we have to get the root and drilldown to the input
		const input = ref.current.firstChild.firstChild.firstChild
		input.focus()
	},[])
	return (
		<Container>
			<p>
				It looks like you were trying to find an entity report, but didn&apos;t
				supply an entity ID. Please enter one in the textbox below.
			</p>

			<Inputs>
				<TextField elementRef={ref} placeholder="sample-220" onChange={handleChange} onKeyUp={handleKey}/>
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

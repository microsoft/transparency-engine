/*!
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project.
 */
const { configure } = require('@essex/webpack-config')
const HtmlWebpackPlugin = require('html-webpack-plugin')

const configuration = configure({
	aliases: () => ({
		'react-router-dom': require.resolve('react-router-dom'),
	}),
	environment: (env, mode) => {
		return {
			APP_MOUNT_PATH: process.env.APP_MOUNT_PATH ?? '/',
		}
	},
	plugins: (env, mode) => [
		new HtmlWebpackPlugin({
			baseUrl: process.env.BASE_URL ?? '/',
			template: './config/index.hbs',
			title: 'Transparency Engine',
			appMountIds: ['root', 'cookie-banner'],
			devServer: mode === 'development',
			files: {
				js: ['https://wcpstatic.microsoft.com/mscc/lib/v2/wcp-consent.js'],
			},
		}),
	],
	devServer: () => ({
		port: 3000,
		allowedHosts: 'all',
	}),
})

// remove old html-plugin
configuration.plugins.shift()
delete configuration.baseUrl
module.exports = configuration

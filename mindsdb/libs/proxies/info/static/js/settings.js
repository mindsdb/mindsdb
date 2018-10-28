const SETTINGS = {
				rebound: {
					tension: 10,
					friction: 7
				},
				spinner: {
					id: 'spinner',
					radius: 150,
					sides: 8,
					depth: 6,
					colors: {
						background: '#181818',
						stroke: '#cccccc',
						base: null,
						child: '#181818'
					},
					alwaysForward: true, // When false the spring will reverse normally.
					restAt: null, // A number from 0.1 to 0.9 || null for full rotation
					renderBase: false
				}
			};
var ResultsComparisonDashboard = new Class({
	Extends: Dashboard,

	getId: function () {
		return "results-comparison";
	},

	getTitle: function () {
		return "Results comparison dashboard";
	},

	getOptions: function () {
		return {
			'Nr. of users': this.usersOption(),
			'Nr. of tenants': this.tenantsOption()
		};
	},

	_getNodeData: function (data, node) {
		var results = [];

		TENANTS.each(function (tenant) {
			var time = 0;
			if (data[node][tenant]) {
				time = Number.from(data[node][tenant]);
			}

			results[results.length] = time;
		});

		return results;
	},

	getHtml: function (data) {
		var container = new Element('div');

		var subtitle = 'Results comparison graph for ' + this.getOptionValue('users') + ' user';
		if (this.getOptionValue('users') > 1) {
			subtitle += 's';
		}
		subtitle += ' and ' + this.getOptionValue('tenants') + ' tenants';

		var series = [];

		this.productList.each(function (product) {
			var serie = {};
			serie.name = product.name;

			serie.data = [];

			NODES.each(function (node) {
				if (typeof(data[product.id]) != "undefined" && typeof(data[product.id][node]) != "undefined") {
					serie.data[serie.data.length] = Number.from(data[product.id][node]['avg_querytime']);
				} else {
					serie.data[serie.data.length] = 0;
				}
			});

			serie.dataLabels = {
				enabled: true,
				align: 'top',
				rotation: -90,
				color: '#000000',
				x: 5,
				y: -5,
				formatter: function() {
					return  this.series.name;
				},
				style: {
					fontSize: '10px',
					fontFamily: 'Verdana, sans-serif'
				}
			};

			series[series.length] = serie;
		});

		var chart1 = new Highcharts.Chart({
			chart: {
				renderTo: container,
				type: 'column',
				width: '900'
			},
            title: {
                text: 'Tenant Graph'
            },
            subtitle: {
                text:  subtitle
            },
            xAxis: {
                categories: NODES,
				title: {text: 'Number of nodes'}
            },
            yAxis: {
                min: 0,
                title: {
                    text: 'Average query execution time (ms)'
                },

            },
            tooltip: {
                headerFormat: '<span style="font-size:10px">{point.key} nodes</span><table>',
                pointFormat: '<tr><td style="color:{series.color};padding:0">{series.name}: </td>' +
                    '<td style="padding:0"><b>{point.y:.0f} ms</b></td></tr>',
                footerFormat: '</table>',
                shared: true,
                useHTML: true
            },
            plotOptions: {
                column: {
                    pointPadding: 0.2,
                    borderWidth: 0
                }
            },
            series: series
        });
		
		return container;
	}

});
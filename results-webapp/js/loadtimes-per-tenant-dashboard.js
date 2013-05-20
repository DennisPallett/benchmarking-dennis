var LoadTimesPerTenantDashboard = new Class({
	Extends: Dashboard,

	getId: function () {
		return "loadtimes-per-tenant";
	},

	getTitle: function () {
		return "Loading times per tenant";
	},

	getOptions: function () {
		return {
			'Product': this.productOption(),
		};
	},

	_getNodeData: function (data, node) {
		var results = [];

		Object.each(data[node], function (row) {
			var time = row.exectime;
			var rowCount = row.rowCount;

			time = time / 1000;

			var rate = rowCount / time;

			results[results.length] = rate;
		});

		// fill results array with dummy value
		while (results.length < 5) {
			results[results.length] = 0;
		}


		return results;
	},

	getHtml: function (data) {
		var container = new Element('div');

		var chart1 = new Highcharts.Chart({
			chart: {
				renderTo: container,
				type: 'column',
				width: '900'
			},
            title: {
                text: 'Loading times per tenant'
            },
            subtitle: {
                text:  "The load rate for tenants per node for " + this.getProductByType(this.getOptionValue('product')).name
            },
            xAxis: {
                categories: ['1-10', '11-20', '21-30', '31-40', '41-50'],
				title: {text: 'Tenants'}
            },
            yAxis: {
                min: 0,
                title: {
                    text: 'Load rate (rows/second)'
                }
            },
            tooltip: {
                headerFormat: '<span style="font-size:10px">tenants {point.key}</span><table>',
                pointFormat: '<tr><td style="color:{series.color};padding:0">{series.name}: </td>' +
                    '<td style="padding:0"><b>{point.y:.0f} rows/second</b></td></tr>',
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
            series: [{
                name: '1 node',
                data: this._getNodeData(data, 1)
    
            }, {
                name: '2 nodes',
                data: this._getNodeData(data, 2)
    
            }, {
                name: '3 nodes',
                data: this._getNodeData(data, 3)
    
            }]
        });
		
		return container;
	}

});
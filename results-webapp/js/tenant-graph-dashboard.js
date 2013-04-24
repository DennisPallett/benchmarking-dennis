var TenantGraphDashboard = new Class({
	Extends: Dashboard,

	getId: function () {
		return "tenant-graph";
	},

	getTitle: function () {
		return "Tenants graph";
	},

	getOptions: function () {
		var tenantsOption = new Element('select', {'name': 'up_to_tenants'});

		TENANTS.each(function (tenant) {
			tenantsOption.adopt(new Element('option', {'html': tenant, 'value': tenant}));
		});

		return {
			'Product': this.productOption(),
			'Nr. of users': this.usersOption(),
			'Up to nr. of tenants': tenantsOption
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

		var subtitle = 'Increasing number of tenants for ' + this.getOptionValue('users') + ' user';
		if (this.getOptionValue('users') > 1) {
			subtitle += 's';
		}
		subtitle += ' up to ' + this.getOptionValue('up_to_tenants') + ' tenants';
		subtitle += ' for ' + PRODUCTS[this.getOptionValue('product')];

		var tenantCats = [];
		var upTo = Number.from(this.getOptionValue('up_to_tenants'));
		TENANTS.each(function (tenant) {
			if (tenant <= upTo) {
				tenantCats[tenantCats.length] = tenant;
			}
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
                categories: tenantCats,
				title: {text: 'Number of tenants'}
            },
            yAxis: {
                min: 0,
                title: {
                    text: 'Average query execution time (ms)'
                }
            },
            tooltip: {
                headerFormat: '<span style="font-size:10px">{point.key} tenants</span><table>',
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
            series: [{
                name: '1 node',
                data: this._getNodeData(data, 1).slice(0, tenantCats.length)
    
            }, {
                name: '2 nodes',
                data: this._getNodeData(data, 2).slice(0, tenantCats.length)
    
            }, {
                name: '3 nodes',
                data: this._getNodeData(data, 3).slice(0, tenantCats.length)
    
            }]
        });
		
		return container;
	}

});
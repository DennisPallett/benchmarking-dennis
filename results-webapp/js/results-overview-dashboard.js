var ResultsOverviewDashboard = new Class({
	Extends: Dashboard,

	getId: function () {
		return "results-overview";
	},

	getTitle: function () {
		return "Results overview";
	},

	getOptions: function () {
		return {
			'Product': this.productOption(),
		};
	},

	_getNodeResult: function (data, tenant, user, node, property) {
		var time = null;

		try {
			time = Number.from(data[tenant][user][node][property]);
		} catch (e) {}

		var ret = '-';
		if (time != null) {
			ret = '';

			ret = time.format({
				'decimal': ',',
				'group': '.'
			});

			if (time > 5000) {
				ret = '<span style="color: red;">' + ret + '</span>';
			}
			if (time <= 1000) {
				ret = '<span style="color: #339900;">' + ret + '</span>';
			}
		}

		return ret;
	},

	getHtml: function (data) {
		var container = new Element('div');

		container.adopt(new Element('h2', {'html': 'Results overview for ' + PRODUCTS[this.getOptionValue('product')]}));

		var table = new Element('table', {'styles': {'text-align': 'center'}});
		var header = new Element('thead');
		var body = new Element('body');

		var headRow = new Element('tr');
		headRow.adopt(new Element('th', {'html': '# of tenants'}));
		headRow.adopt(new Element('th', {'html': '# of users'}));
		headRow.adopt(new Element('th', {'html': '1 node', 'colspan': '2'}));
		headRow.adopt(new Element('th', {'html': '2 nodes', 'colspan': '2'}));
		headRow.adopt(new Element('th', {'html': '3 nodes', 'colspan': '2'}));

		header.adopt(headRow);

		table.adopt(header);

		var subHeadRow = new Element('tr');
		subHeadRow.setStyle('font-weight', 'bold');
		subHeadRow.adopt(new Element('td', {'html': '&nbsp;', 'colspan': '2'}));

		NODES.each(function (node) {
			subHeadRow.adopt(new Element('td', {'html': 'Avg query time (ms)'}));
			subHeadRow.adopt(new Element('td', {'html': 'Avg set time (ms)'}));
		});

		table.adopt(subHeadRow);

		TENANTS.each(function (tenant) {
			var first = true;
			USERS.each(function (user) {
				if (user <= tenant) {
					var row = new Element('tr');

					if (first) {
						row.setStyle('border-top', '3px double #CCC');
					}


					row.adopt(new Element('td', {'html': (first) ? tenant : ''}));
					row.adopt(new Element('td', {'html': user}));

					row.adopt(new Element('td', {'html': this._getNodeResult(data, tenant, user, 1, 'avg_querytime')}));
					row.adopt(new Element('td', {'html': this._getNodeResult(data, tenant, user, 1, 'avg_settime')}));
					row.adopt(new Element('td', {'html': this._getNodeResult(data, tenant, user, 2, 'avg_querytime')}));
					row.adopt(new Element('td', {'html': this._getNodeResult(data, tenant, user, 2, 'avg_settime')}));
					row.adopt(new Element('td', {'html': this._getNodeResult(data, tenant, user, 3, 'avg_querytime')}));
					row.adopt(new Element('td', {'html': this._getNodeResult(data, tenant, user, 3, 'avg_settime')}));

					body.adopt(row);
				}

				if (first) first = false;
			}.bind(this));
		}.bind(this));


		table.adopt(body);	

		container.adopt(table);

		return container.get('html');
	}

});
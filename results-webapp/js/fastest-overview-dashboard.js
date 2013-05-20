var FastestOverviewDashboard = new Class({
	Extends: Dashboard,

	getId: function () {
		return "fastest-overview";
	},

	getTitle: function () {
		return "Fastest overview";
	},

	getOptions: function () {
		return {};
	},

	_getNodeResult: function (data, tenant, user, node, property) {
		var ret = '-';

		try {
			ret = data[tenant][user][node][property];
		} catch (e) {}

		

		return ret;
	},

	getHtml: function (data) {
		var container = new Element('div');

		container.adopt(new Element('h2', {'html': 'Fastest overview'}));

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
			subHeadRow.adopt(new Element('td', {'html': 'Avg query time'}));
			subHeadRow.adopt(new Element('td', {'html': 'Avg set time'}));
		});

		table.adopt(subHeadRow);

		TENANTS.each(function (tenant) {
			var first = true;
			USERS.each(function (user) {
				if (user <= tenant) {
					var row = new Element('tr');

					if (first) {
						row.setStyle('border-top', '3px double #CCC');					}


					row.adopt(new Element('td', {'html': (first) ? tenant : ''}));
					row.adopt(new Element('td', {'html': user}));

					row.adopt(new Element('td', {'html': this._getNodeResult(data, tenant, user, 1, 'fastest_product_query')}));
					row.adopt(new Element('td', {'html': this._getNodeResult(data, tenant, user, 1, 'fastest_product_set')}));
					row.adopt(new Element('td', {'html': this._getNodeResult(data, tenant, user, 2, 'fastest_product_query')}));
					row.adopt(new Element('td', {'html': this._getNodeResult(data, tenant, user, 2, 'fastest_product_set')}));
					row.adopt(new Element('td', {'html': this._getNodeResult(data, tenant, user, 3, 'fastest_product_query')}));
					row.adopt(new Element('td', {'html': this._getNodeResult(data, tenant, user, 3, 'fastest_product_set')}));

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
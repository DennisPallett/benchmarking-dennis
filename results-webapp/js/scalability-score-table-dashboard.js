var ScalabilityScoreTableDashboard = new Class({
	Extends: Dashboard,

	getId: function () {
		return "scalability-score-table";
	},

	getTitle: function () {
		return "Scalability score table";
	},

	getOptions: function () {
		return {
			'Product': this.productOption()
		};
	},

	_printNodeResult: function (tenant, user, node, row, data) {
		if (node > 1) {
			var expectedTime = '-';
			try {
				expectedTime = Number.from(data[tenant][user][node]['expected']['avg_querytime']);
				expectedTime = expectedTime.format({
					decimals: 0
				});
			} catch (e) {}
			row.adopt(new Element('td', {'html': expectedTime, 'styles': {'border-left': '3px double #CCC'}}));
		}		

		var actualTime = '-';
		try {
			actualTime = Number.from(data[tenant][user][node]['actual']['avg_querytime']);
		} catch (e) {}
		row.adopt(new Element('td', {'html': actualTime}));

		if (node > 1) {
			var score = '-';
			try {
				score = Number.from(data[tenant][user][node]['query_score']);
				score = score.format({
					decimals: 2
				});
			} catch (e) {}
			row.adopt(new Element('td', {'html': score, 'styles': {'border-right': '3px double #CCC'}}));
		}

	},

	getHtml: function (data) {
		var container = new Element('div');

		container.adopt(new Element('h2', {'html': 'Scalability score table for ' + PRODUCTS[this.getOptionValue('product')]}));

		var score = Number.from(data['query_score']);
		score = score.format({
			decimals: 2
		});
		container.adopt(new Element('p', {'html': '<strong>Overall scalability score:</strong> ' + score, 'styles': {'text-align': 'center'}}));

		var table = new Element('table', {'styles': {'text-align': 'center'}});
		var header = new Element('thead');
		var body = new Element('body');

		var headRow = new Element('tr');
		headRow.adopt(new Element('th', {'html': '# of tenants'}));
		headRow.adopt(new Element('th', {'html': '# of users', 'styles': {'border-right': '3px double #CCC'}}));
		headRow.adopt(new Element('th', {'html': '1 node', 'colspan': '1', 'styles': {'border-right': '3px double #CCC'}}));
		headRow.adopt(new Element('th', {'html': '2 nodes', 'colspan': '3', 'styles': {'border-right': '3px double #CCC'}}));
		headRow.adopt(new Element('th', {'html': '3 nodes', 'colspan': '3', 'styles': {'border-right': '3px double #CCC'}}));
		headRow.adopt(new Element('th', {'html': 'Score'}));

		header.adopt(headRow);

		table.adopt(header);

		var subHeadRow = new Element('tr');
		subHeadRow.setStyle('font-weight', 'bold');
		subHeadRow.adopt(new Element('td', {'html': '&nbsp;', 'colspan': '2', 'styles': {'border-right': '3px double #CCC'}}));

		subHeadRow.adopt(new Element('td', {'html': 'Actual time (ms)', 'styles': {'border-right': '3px double #CCC'}}));

		NODES.each(function (node) {
			if (node > 1) {
				subHeadRow.adopt(new Element('td', {'html': 'Expected time (ms)'}));
				subHeadRow.adopt(new Element('td', {'html': 'Actual time (ms)'}));
				subHeadRow.adopt(new Element('td', {'html': 'Score', 'styles': {'border-right': '3px double #CCC'}}));
			}
		});

		subHeadRow.adopt(new Element('td', {'html': '&nbsp;'}));


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
					row.adopt(new Element('td', {'html': user, 'styles': {'border-right': '3px double #CCC'}}));

					this._printNodeResult(tenant, user, 1, row, data);
					this._printNodeResult(tenant, user, 2, row, data);
					this._printNodeResult(tenant, user, 3, row, data);

					var score = '-';
					try {
						score = Number.from(data[tenant][user]['query_score']);
						score = score.format({
							decimals: 2
						});
					} catch (e) {}

					row.adopt(new Element('td', {'html': score}));

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
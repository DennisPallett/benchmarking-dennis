var TENANTS = [1, 2, 5, 10, 20, 30, 40, 50, 75, 100];
var USERS = [1, 2, 5, 10, 20, 30, 40, 50, 75, 100];
var NODES = [1, 2, 3];

var PRODUCTS = {'vertica': 'HP Vertica'};

$(window).addEvent('load', function () {
	var app = new DashboardApp();
});


var DashboardApp = new Class({
	dashboards: {},
    selector: null,
	optionsForm: null,

	currDashboard: null,

	initialize: function () {
		this.dashboards = [
			new ResultsOverviewDashboard(),
			new TenantGraphDashboard(),
			new LoadTimesPerTenantDashboard(),
			new ScalabilityScoreTableDashboard()

			/*,
			["Scalability", [
				new ScalabilityScoreDashboard(this)
			]]*/
		];

		this._setupSelector();
		this._setupOptions();

		this._loadInitial();
	},

	_setupSelector: function () {
		this.selector = $('selector').getElement('select');

		this.selector.adopt(new Element('option', {'html': '-- select a dashboard --'}));

		this.dashboards.each(function (item, index) {
			if (item instanceof Dashboard) {
				this.selector.adopt(new Element('option', {'html': item.getTitle(), 'value': index}));
			} else {
				var group = new Element('optgroup', {'label': item[0]});

				for(var i=0; i < item[1].length; i++) {
					group.adopt(new Element('option', {'html': item[1][i].getTitle(), 'value': index + '_' + i}));
				}

				this.selector.adopt(group);
			}
		}.bind(this));


		this.selector.addEvent("change", this.selectDashboard.bind(this));
	},

	_setupOptions: function () {
		this.optionsForm = $('options');


		this.optionsForm.getElement('input[type="button"]').addEvent('click', this.saveOptions.bind(this));
	},

	_loadInitial: function () {
		var hash = document.location.hash;

		if (hash.substring(0, 1) == '#') {
			hash = hash.substring(1);
		}

		var split = hash.split(',');

		split.each(function (item) {
			if (item.indexOf('dashboard') > -1) {
				var temp = item.split('-');
				var index = temp[1];

				this.selector.value = index;
				this.selectDashboard();
			}
		}.bind(this));
	},

	saveOptions: function (e) {
		// reload current dashboard
		this.loadDashboard(this.currDashboard);
	},

	selectDashboard: function () {
		var el = this.selector;		

		if (el.selectedIndex == 0) {
			// hide everything
			$('dashboard').setStyle('display', 'none');
			$('loading').setStyle('display', 'none');
			$('options_container').setStyle('display', 'none');

			$('select_dashboard').setStyle('display', 'block');
			this.setTitle('');
			return;
		} 

		// is a group index or not?
		var dashboard = null;
		if (el.value.indexOf('_') < 0) {
			dashboard = this.dashboards[el.value];
		} else {
			var split = el.value.split('_');
			var groupIndex = split[0];
			var itemIndex = split[1];
			dashboard = this.dashboards[groupIndex][1][itemIndex];
		}

		if (dashboard == null) {
			alert('ERROR: unable to load dashboard!');
			return false;
		}

		history.pushState({}, dashboard.getTitle(), '#dashboard-' + el.value);
		this.loadDashboard(dashboard);
		
	},

	loadDashboard: function (dashboard) {
		this.setTitle(dashboard.getTitle());		

		$('select_dashboard').setStyle('display', 'none');
		$('loading').setStyle('display', 'block');
		$('dashboard').setStyle('display', 'none').set('html', '');
		$('options_container').setStyle('display', 'none');

		var url = 'data.php?';
		url += 'id=' + dashboard.getId();
		url += '&' + this.optionsForm.toQueryString(); 

		var req = new Request.JSON({
			'url': url,
			'method': 'get',
			'onSuccess': function (json, text) {
				if (json.error != undefined) {
					// todo: show pretty error
					alert('ERROR: ' + json.error);
				} else {
					this.showDashboard(dashboard, json);
				}
			}.bind(this)
		});

		req.send();
	},

	showDashboard: function (dashboard, json) {
		$('loading').setStyle('display', 'none');

		this.currDashboard = dashboard;

		var options = dashboard.getOptions();

		dashboard.setOptionValues(json.options);
		var html = dashboard.getHtml(json.data);

		if (html instanceof Object) {
			$('dashboard').adopt(html);
		} else {
			$('dashboard').set('html', html);
		}

		if (Object.getLength(options) > 0) {
			this.showOptions(options, json.options);
		}

		
		$('dashboard').setStyle('display', 'block');
	},

	showOptions: function (options, optionValues) {
		if (Object.getLength(options) == 0) return;

		var container = this.optionsForm.getElement('table');

		container.set('html', '');

		Object.each(options, function (field, label) {
			var row = new Element('tr');
			row.adopt(new Element('th', {'html': label}));

			var fieldEl = new Element('td');
			if (field instanceof Object) {
				fieldEl.adopt(field);
			} else {
				fieldEl.set('html', field);
			}
			row.adopt(fieldEl);

			container.adopt(row);
		});

		Object.each(optionValues, function (value, key) {
			var element = this.optionsForm[key];
			if (element) element.value = value;
		}.bind(this));

		$('options_container').setStyle('display', 'block');
	},

	setTitle: function (newTitle) {
		if (newTitle.length > 0) newTitle = ' :: ' + newTitle;
		$('subtitle').set('text', newTitle);
	}
});


var Dashboard = new Class({
	optionValues: {},

	setOptionValues: function (optionValues) {
		this.optionValues = optionValues;
	},

	getOptionValue: function (key, defaultValue) {
		return (this.optionValues[key]) ? this.optionValues[key] : defaultValue;
	},

	getTitle: function () {
		return "TODO: implement getTitle() by child class!";
	},

	load: function () {
		return {options: {}, html: ''};
	},

	getId: function () {

	},

	usersOption: function () {
		var select = new Element('select', {'name': 'users'});

		USERS.each(function (user) {
			select.adopt(new Element('option', {'html': user, 'value': user}));
		});

		return select;
	},

	productOption: function () {
		var select = new Element('select', {'name': 'product'});

		Object.each(PRODUCTS, function (label, key) {
			select.adopt(new Element('option', {'html': label, 'value': key}));
		});

		return select;
	}
});



var ScalabilityScoreDashboard = new Class({
	Extends: Dashboard,
	
	getTitle: function () {
		return "Score overview";
	},

	load: function () {
		this.app.showDashboard({bla: 'bla'}, 'dashboard!');
	}
});
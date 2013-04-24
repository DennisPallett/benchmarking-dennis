<?php

class DashboardsApplication {
	protected $dashboards = array();
	protected $_config = array();
	protected $_options = array();
	protected $_optionValues = array();
	protected $db;

	protected $id;

	public function __construct () {
		$this->_loadConfig();

		$this->_setupDbConnection();

		$this->addDashboard('results-overview', new ResultsOverviewDashboard());
		$this->addDashboard('tenant-graph', new TenantGraphDashboard());
		$this->addDashboard('loadtimes-per-tenant', new LoadtimesPerTenantDashboard());
		$this->addDashboard('scalability-score-table', new ScalabilityScoreTableDashboard());

		$this->addOption('users', '1', 'ctype_digit');
		$this->addOption('tenants', '1', 'ctype_digit');
		$this->addOption('product', 'vertica');
	}

	public function addDashboard($id, Dashboard $dashboard) {
		$dashboard->setDatabase($this->db);
		$dashboard->setApplication($this);
		$this->dashboards[$id] = $dashboard;

		$dashboard->addOptions();
	}

	public function addOption ($key, $default=null, $validateCallback=false) {
		$this->_options[] = array($key, $default, $validateCallback);
	}

	public function run () {
		// validate dashboard id
		$id = $this->_getParam('id');
		if (empty($id) || !isset($this->dashboards[$id])) {
			throw new InvalidIdException();
		}

		// parse options
		foreach($this->_options as $item) {
			list($key, $default, $validateCallback) = $item;

			$value = $this->_getParam($key, $default);

			if (is_callable($validateCallback)) {
				if (!$validateCallback($value)) $value = $default;
			}

			$this->_optionValues[$key] = $value;
		}

		$dashboard = $this->dashboards[$id];

		$dashboard->setOptionValues($this->_optionValues);

		$data = $dashboard->loadData();


		$ret = array('data' => $data, 'options' => $dashboard->getOptionsUsed());
		return $ret;
	}

	public function getConfig($key, $default=null) {
		$split = explode('.', $key);

		$currArray = $this->_config;
		foreach($split as $key) {
			if (!isset($currArray[$key])) {
				return $default;
			} else {
				$currArray = $currArray[$key];
			}
		}

		return $currArray;
	}

	protected function _getParam($key, $default=null) {
		return (isset($_GET[$key])) ? $_GET[$key] : $default;
	}

	protected function _setupDbConnection () {
		$dsn = 'mysql:dbname=' . $this->getConfig('database.name') . ';host=' . $this->getConfig('database.host', 'localhost');
		$this->db = new PDO($dsn, $this->getConfig('database.user'), $this->getConfig('database.password'));
	}

	protected function _loadConfig() {
		$config = parse_ini_file('config.ini', true);

		if (!isset($config['database'])) {
			throw new InvalidConfigException();
		}

		$this->_config = $config;
	}
}

class InvalidIdException extends Exception {}
class InvalidConfigException extends Exception {}
class InvalidProductException extends Exception {}

abstract class Dashboard {
	protected $db;
	protected $app;
	protected $_optionValues = array();
	protected $_optionsUsed = array();

	public function setDatabase($db) {
		$this->db = $db;
	}

	public function addOptions () {
		//
	}

	public function setApplication ($app) {
		$this->app = $app;
	}

	public function setOptionValues ($values) {
		$this->_optionValues = $values;
	}

	public function getOptionsUsed () {
		return $this->_optionsUsed;
	}

	public function getOptionValue ($key, $default=null) {
		$value = (isset($this->_optionValues[$key])) ? $this->_optionValues[$key] : $default;

		$this->_optionsUsed[$key] = $value;

		return $value;
	}

	public function getProductId () {
		$type = $this->getOptionValue('product', 'vertica');

		$q = $this->db->prepare("SELECT * FROM product WHERE product_type = :type");
		$q->bindValue(":type", $type);

		$q->execute();

		$results = $q->fetch();

		if (empty($results)) {
			throw new InvalidProductException();
		}

		return intval($results['product_id']);
	}

	protected function _getParam($key, $default=null) {
		return (isset($_GET[$key])) ? $_GET[$key] : $default;
	}

	protected function _calculateInstanceScore ($cpuTime, $memoryTime, $fileTime, $oltpTime) {
		// just use OLTP as instance score
		return $oltpTime;
	}

	abstract function loadData();
}

class ResultsOverviewDashboard extends Dashboard {
	
	public function loadData() {
		$q = $this->db->prepare("
			SELECT * FROM benchmark
			WHERE benchmark_product = :product
		");

		$q->bindValue(':product', $this->getProductId());
		$q->execute();

		$results = $q->fetchAll(PDO::FETCH_ASSOC);

		$ret = array();

		foreach($results as $row) {
			$key = $row['benchmark_tenants'] . '-' . $row['benchmark_users'] . '-' . $row['benchmark_nodes'];
			$tenants = $row['benchmark_tenants'];
			$users = $row['benchmark_users'];
			$nodes = $row['benchmark_nodes'];

			$ret[$tenants][$users][$nodes] = array(
				'avg_querytime' => $row['benchmark_avg_querytime'],
				'avg_settime'	=> $row['benchmark_avg_settime']
			);

		}

		return $ret;
	}

}

class TenantGraphDashboard extends Dashboard {

	public function addOptions () {
		$this->app->addOption('up_to_tenants', 100, 'ctype_digit');
	}

	public function loadData () {
		$q = $this->db->prepare("
			SELECT * FROM benchmark 
			WHERE benchmark_users = :users 
			AND benchmark_tenants <= :up_to_tenants
			AND benchmark_product = :product

			ORDER BY benchmark_tenants ASC
		");
		
		$q->bindValue(':users', intval($this->getOptionValue('users', 1)));
		$q->bindValue(':up_to_tenants', intval($this->getOptionValue('up_to_tenants', 100)));
		$q->bindValue(':product', $this->getProductId());
		$q->execute();

		$results = $q->fetchAll();

		$ret = array();
		$ret['1'] = array();
		$ret['2'] = array();
		$ret['3'] = array();

		foreach($results as $row) {
			$nodes = $row['benchmark_nodes'];
			$tenants = $row['benchmark_tenants'];

			$ret[$nodes][$tenants] = $row['benchmark_avg_querytime'];
		}

		return $ret;
	}
}

class LoadTimesPerTenantDashboard extends Dashboard {

	public function loadData () {
		$q = $this->db->prepare("
			SELECT load_nodes AS nodeId, FLOOR((load_tenant-1)/10)+1 AS groupId, SUM(rowCount) AS rowCount, SUM(exectime) AS exectime
			FROM `load`
			INNER JOIN load_results ON `load` = load_id AND `table` = 'fact_exploitatie'
			WHERE load_product = :product
			GROUP BY load_nodes, FLOOR((`load_tenant`-1) / 10)
		");
		
		//$q->bindValue(':users', intval($this->getOptionValue('users', 1)));
		//$q->bindValue(':up_to_tenants', intval($this->getOptionValue('up_to_tenants', 100)));
		$q->bindValue(':product', $this->getProductId());
		$q->execute();

		$results = $q->fetchAll();

		$ret['1'] = array();
		$ret['2'] = array();
		$ret['3'] = array();
		foreach($results as $row) {
			$node = $row['nodeId'];
			$groupId = $row['groupId'];

			$ret[$node][$groupId] = array('rowCount' => $row['rowCount'], 'exectime' => $row['exectime']);
		}

		return $ret;
	}
}

class ScalabilityScoreTableDashboard extends Dashboard {
	
	public function loadData() {
		$q = $this->db->prepare("
			SELECT * FROM benchmark
			WHERE benchmark_product = :product
		");

		$q->bindValue(':product', $this->getProductId());
		$q->execute();

		$results = $q->fetchAll(PDO::FETCH_ASSOC);

		$ret = array();
		
		// get actual results from database
		$benchmarkIds = array(0);
		foreach($results as $row) {
			$benchmarkIds[] = $row['benchmark_id'];

			$key = $row['benchmark_tenants'] . '-' . $row['benchmark_users'] . '-' . $row['benchmark_nodes'];
			$tenants = $row['benchmark_tenants'];
			$users = $row['benchmark_users'];
			$nodes = $row['benchmark_nodes'];

			$ret[$tenants][$users][$nodes]['actual'] = array(
				'avg_querytime' => $row['benchmark_avg_querytime'],
				'avg_settime'	=> $row['benchmark_avg_settime']
			);

		}

		// get instances related to benchmarks
		$q = $this->db->prepare("
			SELECT * FROM instance
			INNER JOIN benchmark_instance ON instance_id = instance
			WHERE benchmark IN (" . implode(',', $benchmarkIds) . ")
			GROUP BY instance_id
		");

		$q->execute();
		$results = $q->fetchAll();

		$instances = array();
		foreach($results as $row) {
			$row['score'] = $this->_calculateInstanceScore($row['instance_cpu'], $row['instance_memory'], $row['instance_fileio'], $row['instance_oltp']);
			$node = $row['instance_node'];

			$instances[$node] = $row;
		}

		// calculate expected results
		$globalQueryScoreList = array();
		$globalSetScoreList = array();
		foreach($ret as $tenant => $users) {
			foreach($users as $user => $nodes) {
				$benchmarkQueryScoreList = array();
				$benchmarkSetScoreList = array();

				foreach($nodes as $node => $row) {
					$querytime = 0;
					$settime = 0;

					$prevNodeId = $node - 1;
					if (isset($ret[$tenant][$user][$prevNodeId])) {
						// fetch values of previous node
						$prevNode = $ret[$tenant][$user][$prevNodeId];
						$prevQueryTime = $prevNode['actual']['avg_querytime'];
						$prevSetTime = $prevNode['actual']['avg_settime'];

						// calculated expected values
						$querytime = $prevQueryTime * ($prevNodeId / $node);
						$settime = $prevSetTime * ($prevNodeId / $node);
					}

					$ret[$tenant][$user][$node]['expected'] = array(
						'avg_querytime'	=> $querytime,
						'avg_settime'	=> $settime
					);

					// calculate scalability scores for this node
					$queryScore = $querytime / $ret[$tenant][$user][$node]['actual']['avg_querytime'];
					$setScore = $settime / $ret[$tenant][$user][$node]['actual']['avg_settime'];

					$ret[$tenant][$user][$node]['query_score'] = $queryScore;
					$ret[$tenant][$user][$node]['set_score'] = $setScore;

					if ($queryScore > 0 && $setScore > 0) {
						$benchmarkQueryScoreList[] = $queryScore;
						$benchmarkSetScoreList[] = $setScore;
					}
				}

				// calculate scores for this benchmark (average of all nodes)
				$benchmarkQueryScore = 0;
				if (count($benchmarkQueryScoreList) > 0) {
					$benchmarkQueryScore = array_sum($benchmarkQueryScoreList) / count($benchmarkQueryScoreList);
				}
				$benchmarkSetScore = 0;
				if (count($benchmarkSetScoreList) > 0) {
					$benchmarkSetScore = array_sum($benchmarkSetScoreList) / count($benchmarkSetScoreList);
				}

				if ($benchmarkQueryScore > 0) {
					$globalQueryScoreList[] = $benchmarkQueryScore;
				}

				if ($benchmarkSetScore > 0) {
					$globalSetScoreList[] = $benchmarkSetScore;
				}				

				$ret[$tenant][$user]['query_score'] = $benchmarkQueryScore;
				$ret[$tenant][$user]['set_score'] = $benchmarkSetScore;
			}
		}

		// calculate global scalability scores for this product
		$globalQueryScore = 0;
		if (count($globalQueryScoreList) > 0) {
			$globalQueryScore = array_sum($globalQueryScoreList) / count($globalQueryScoreList);
		}


		$globalSetScore = 0;
		if (count($globalSetScoreList) > 0) {
			$globalSetScore = array_sum($globalSetScoreList) / count($globalSetScoreList);
		}

		$ret['query_score'] = $globalQueryScore;
		$ret['set_score'] = $globalSetScore;

		return $ret;
	}

}



header('Content-Type: text/plain');

$app = new DashboardsApplication ();

try {
	echo json_encode($app->run());
} catch (Exception $e) {
	$ret = array('error' => get_class($e), 'message' => $e->getMessage());
	echo json_encode($ret);
}
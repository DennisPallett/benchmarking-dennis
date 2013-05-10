package topicus.benchmarking;

import java.util.List;

import topicus.databases.AbstractDatabase;

public abstract class AbstractBenchmarkRunner {
	public final static int NR_OF_QUERIES = 4;
	
	protected AbstractDatabase database;
	protected int userId;
	protected BenchmarksScript owner;
	protected int nodes;
	protected List<String[]> queryList;
	
	public abstract void prepareBenchmark () throws PrepareException;
	public abstract void runIteration (int iteration);
	public abstract void finishBenchmark ();
	
	public void setQueryList(List<String[]> queryList) {
		this.queryList = queryList;
	}
	
	public void setOwner(BenchmarksScript owner) {
		this.owner = owner;
	}
	
	public void setNodes(int nodes) {
		this.nodes = nodes;
	}
	
	public void setUserId(int userId) {
		this.userId = userId;
	}
	
	public void setDatabase(AbstractDatabase database) {
		this.database = database;
	}
	
	protected String _replaceOrgId(String query) {
		// replace all organisation ID's with tenant's org ids
				
		// number of rows in the organisation table
		final int ORG_ROW_COUNT = 988;
		
		// id's in queries
		int[] ids = {752, 755, 756, 799};
		
		// replace each ID
		for(int id : ids) {
			query = query.replaceAll(String.valueOf(id), String.valueOf((this.userId-1) * ORG_ROW_COUNT + id));
		}		
		
		return query;
	}

	
	
	public class PrepareException extends Exception {
		public PrepareException(String string) {
			super(string);
		}
	}

}

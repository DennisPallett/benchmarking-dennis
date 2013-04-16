package topicus.benchmarking;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;

import topicus.ConsoleScript;

public class BenchmarkInstanceScript extends ConsoleScript {
	public static final int NR_OF_RUNS = 4;
	
	protected int maxPrime = 20000;
	protected int memorySize = 100;
	protected String fileSize = "10G";
	protected int maxOltpRequests = 50000;
	
	protected String benchmarkDirectory;
	protected String bucketName;
	protected String awsCredentialsFile;
	protected String hostName;
	protected File resultsFile;
		
	protected AmazonS3Client s3Client;
	
	
	protected boolean testMode = false;

	public void run () throws InvalidBenchmarkDirectoryException, InvalidBucketException, MissingAwsCredentialsException, HostNameException, IOException, CancelledException {
		printLine("Started-up benchmark instance tool");
		
		this._setOptions();
		
		// check if start
		if (!this.cliArgs.hasOption("start")) {
			printLine("Ready to start benchmark");
			if (!confirmBoolean("Start instance benchmarks? (y/n)")) {
				throw new CancelledException("Cancelled by user");
			}
		}
		
		this._runCpuBenchmarks();
		this._runMemoryBenchmarks();
		if (testMode == false) {
			this._runFileBenchmarks();
		}
		this._runOltpBenchmarks();
		
		// upload results file to S3
		printLine("Uploading results to S3 in bucket `" + bucketName + "`");
		s3Client.putObject(this.bucketName, resultsFile.getName(), resultsFile);
		printLine("Upload complete");
		
		printLine("Benchmark finished!");
	}
	
	protected void _runCpuBenchmarks () throws IOException {
		printLine("Running CPU benchmarks");
		
		ArrayList<String> commands = new ArrayList<String>();
		
		commands.add("--test=cpu");
		commands.add("--cpu-max-prime=" + maxPrime);
		
		for(int i=1; i <= NR_OF_RUNS; i++) {
			printLine("CPU run " + i);
			long start = System.currentTimeMillis();
			
			String result = _runBenchmark(commands);
			System.out.print(result);
			
			long time = System.currentTimeMillis() - start;			
			printLine("Finished CPU run " + i + " in " + (time/1000) + " s");
			
			_saveResult("CPU", i, result);
		}
		
		printLine("Finished CPU benchmarks");
	}
	
	protected void _runMemoryBenchmarks () throws IOException {
		printLine("Running memory benchmarks");
		
		ArrayList<String> commands = new ArrayList<String>();
		
		commands.add("--test=memory");
		commands.add("--memory-total-size=" + memorySize + "G");
		
		for(int i=1; i <= NR_OF_RUNS; i++) {
			printLine("Memory run " + i);
			long start = System.currentTimeMillis();
			
			String result = _runBenchmark(commands);
			System.out.print(result);
			
			long time = System.currentTimeMillis() - start;			
			printLine("Finished memory run " + i + " in " + (time/1000) + " s");
			
			_saveResult("Memory", i, result);
		}
		
		printLine("Finished memory benchmarks");
	}
	
	protected void _runFileBenchmarks () throws IOException {
		printLine("Running file I/O benchmarks");
		
		ArrayList<String> commands = new ArrayList<String>();
		
		commands.add("--test=fileio");
		commands.add("--file-total-size=" + this.fileSize);
		commands.add("--file-test-mode=rndrw");
		commands.add("--init-rng=on");
		commands.add("--num-threads=16");		
		
		for(int i=1; i <= NR_OF_RUNS; i++) {
			printLine("File I/O run " + i);
			long start = System.currentTimeMillis();
			
			String result = _runBenchmark(commands);
			System.out.print(result);
			
			long time = System.currentTimeMillis() - start;			
			printLine("Finished file I/O run " + i + " in " + (time/1000) + " s");
			
			_saveResult("File", i, result);
		}
		
		printLine("Finished file I/O benchmarks");
	}	
	
	protected void _runOltpBenchmarks () throws IOException {
		printLine("Running OLTP benchmarks");
		
		ArrayList<String> commands = new ArrayList<String>();
		
		commands.add("--test=oltp");
		commands.add("--num-threads=16");	
		commands.add("--max-requests=" + this.maxOltpRequests);
		commands.add("--oltp-table-size=1000000");
		commands.add("--mysql-user=root");
		commands.add("--mysql-password=sa");
		commands.add("--mysql-db=sbtest");
		commands.add("--oltp-read-only=on");			
		
		for(int i=1; i <= NR_OF_RUNS; i++) {
			printLine("OLTP run " + i);
			long start = System.currentTimeMillis();
			
			String result = _runBenchmark(commands);
			System.out.print(result);
			
			long time = System.currentTimeMillis() - start;			
			printLine("Finished OLTP run " + i + " in " + (time/1000) + " s");
			
			_saveResult("OLTP", i, result);
		}
		
		printLine("Finished OLTP benchmarks");
	}	
	
	protected String _runBenchmark (ArrayList<String> inputCommands) {
		ArrayList commands = (ArrayList) inputCommands.clone();

		commands.add(0, "sysbench");
		commands.add("run");
		
		ProcessBuilder pb = new ProcessBuilder(commands);
		pb.directory(new File(this.benchmarkDirectory));
		
		String result;
		try {
			result = exec(pb.start());
		} catch (IOException e) {
			result = "FAIL";
		}
		return result;
	}
	
	protected void _saveResult(String test, int run, String result) throws IOException {
		StringBuilder data = new StringBuilder();
		data.append("===" + test + "-" + run + "===");
		data.append("\n");
		data.append(result);
		data.append("\n");
		data.append("======");
		data.append("\n");
		
		// append to results file
		FileUtils.writeStringToFile(resultsFile, data.toString(), true);
	}
	
	protected void _setOptions () throws InvalidBenchmarkDirectoryException, InvalidBucketException, MissingAwsCredentialsException, HostNameException {
		this.benchmarkDirectory = cliArgs.getOptionValue("benchmark-directory", "/mnt/benchmark-instance");
		File benchmarkDir = new File(this.benchmarkDirectory);
		if (benchmarkDir.exists() == false || benchmarkDir.isFile()) {
			throw new InvalidBenchmarkDirectoryException("Benchmark directory `" + benchmarkDirectory + "` is invalid or does not exist");
		}
		this.benchmarkDirectory = benchmarkDir.getAbsolutePath() + "/";
		printLine("Benchmark directory set to: " + benchmarkDirectory);
		
		this.awsCredentialsFile = cliArgs.getOptionValue("aws-credentials", System.getProperty("user.home") + "/AwsCredentials.properties");
		File awsFile = new File(this.awsCredentialsFile);
		if (awsFile.exists() == false) {
			throw new MissingAwsCredentialsException("Missing AWS credentials file");
		}
		printLine("Using AWS credentials: " + this.awsCredentialsFile);
		
		this.bucketName = cliArgs.getOptionValue("bucket", "instance-benchmarks");
		if (this.bucketName.length() == 0) {
			throw new InvalidBucketException("S3 bucket name cannot be empty!");
		}
		
		// create AWS credentials property
		PropertiesCredentials awsProps;
		try {
			awsProps = new PropertiesCredentials(awsFile);
		} catch (Exception e) {
			throw new MissingAwsCredentialsException("Invalid AWS credentials file");
		}
		
		// setup S3 client
		s3Client = new AmazonS3Client(awsProps);
		s3Client.setRegion(Region.getRegion(Regions.EU_WEST_1));
		
		// check if bucket exists
		if (s3Client.doesBucketExist(this.bucketName) == false) {
			throw new InvalidBucketException("Specified S3 bucket `" + this.bucketName + "` does not exist");
		}
		
		printLine("Using S3 bucket: " + this.bucketName);
		
		// get hostname
		try {
			this.hostName = this.exec("hostname").trim();
		} catch (IOException e) {
			throw new HostNameException("Unable to get hostname!");
		}
		
		// create filename for results
		this.resultsFile = new File(this.benchmarkDirectory + "benchmark-" + hostName + "-" + 
				new SimpleDateFormat("dd.MM.yyyy.HH.mm").format(Calendar.getInstance().getTime()) + ".dat");
		printLine("Saving results to: " + this.resultsFile.getAbsolutePath());
		
		testMode = cliArgs.hasOption("test-mode");
		if (testMode) {
			printLine("Test mode enabled!");
			this.maxPrime = 2000;
			this.memorySize = 2;
			this.fileSize = "100MB";
			this.maxOltpRequests = 1000;
		}
	}
	
	public class InvalidBenchmarkDirectoryException extends Exception {
		public InvalidBenchmarkDirectoryException(String string) {
			super(string);
		}
	}
	
	public class MissingAwsCredentialsException extends Exception {
		public MissingAwsCredentialsException(String string) {
			super(string);
		}
	}
	
	public class InvalidBucketException extends Exception {
		public InvalidBucketException(String string) {
			super(string);
		}
	}
	
	public class HostNameException extends Exception {
		public HostNameException(String string) {
			super(string);
		}
	}
	
	public class CancelledException extends Exception {
		public CancelledException(String string) {
			super(string);
		}
	}
}

package topicus;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.commons.cli.CommandLine;

public abstract class ConsoleScript {
	protected String title;
	protected CommandLine cliArgs;
	
	protected BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
	
	protected String logBuffer = "";
	protected FileWriter logOut = null;
	
	public String getTitle () {
		return this.title;
	}
	
	public void setCliArgs (CommandLine cliArgs) {
		this.cliArgs = cliArgs;
	}
		
	public void printError (String msg) {
		this.printLine(msg, true, true);
	}
	
	public void printLine(String msg) {
		this.printLine(msg, true, false);
	}
	
	protected void setupLogging (String filename) throws IOException {
		// setup logging
		this.logOut = new FileWriter(filename);
		this.printLine("Logfile setup");
	}
	
	public String confirm(String msg) throws IOException {
		System.out.print(msg + "  ");
		String ret = stdin.readLine();
		System.out.println("");
		return ret;
	}
	
	public boolean confirmBoolean(String msg) throws IOException {
		boolean ret = false;
		String input = this.confirm(msg);
		
		input = input.toLowerCase();
		
		return (input.charAt(0) == 'y');
	}
	
	public void printLine (String msg, boolean newLine, boolean isError) {
		StringBuilder output = new StringBuilder();
		
		// build output message
		output.append("[");
		output.append(new SimpleDateFormat("HH:mm:ss:SS").format(Calendar.getInstance().getTime()));
		output.append("] ");		
		output.append(msg);
		
		if (newLine) {
			output.append("\n");
		}		
		
		// print to console
		if (isError) {
			System.err.print(output);
		} else {
			System.out.print(output);
		}
		
		logBuffer += output;
			
		// save to log file
		if (this.logOut != null) {
			try {
				this.logOut.write(logBuffer);
				this.logOut.flush();
			} catch (IOException e) {
				System.err.println("ERROR: unable to write to log file");
				System.exit(0);
			}
			
						
			logBuffer = "";
		}
	}

}

package org.littlestar.tpcc;

public class Benchmark {
	public static final String BENCHMARK_COMMAND = "benchmark";
	public static final String LOAD_COMMAND = "load";
	public static final String CHECK_COMMAND = "check";
	public static final String DROP_COMMAND = "drop";
	public static final String ADDFK_COMMAND = "addfk";
	public static final String DROPFK_COMMAND = "dropfk";
	public static final String GATHERSTATS_COMMAND = "gather";
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			throw new IllegalArgumentException(
					"usage: org.littlestar.tpcc.Benchmark {benchmark-config-file} {benchmark|load|drop|addfk|dropfk|check|gather}");
		} else {
			String configFile = args[0];
			String command = args[1];
			new BenchmarkBuilder(configFile).execute(command);
		}
	}
}
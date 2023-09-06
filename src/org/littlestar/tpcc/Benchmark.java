package org.littlestar.tpcc;

import java.util.Objects;

public class Benchmark {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			throw new IllegalArgumentException(
					"usage: org.littlestar.tpcc.Benchmark {benchmark-config-file} {benchmark|load|drop|addfk|dropfk|check}");
		} else {
			String configFile = args[0];
			BenchmarkBuilder builder = new BenchmarkBuilder(configFile);
			String command = args[1];
			if (Objects.equals(command, "benchmark")) {
				builder.doBenchmark();
			} else if (Objects.equals(command, "load")) {
				builder.doLoad(true, true);
			} else if (Objects.equals(command, "check")) {
				builder.doCheck();
			} else if (Objects.equals(command, "drop")) {
				builder.doDrop();
			} else if (Objects.equals(command, "addfk")) {
				builder.doAddFk();
			} else if (Objects.equals(command, "dropfk")) {
				builder.doDropFk();
			} else {
				throw new IllegalArgumentException("command only accept: {benchmark|load|drop|check}");
			}
		}
	}
}

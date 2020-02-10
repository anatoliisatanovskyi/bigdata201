package anatoliisatanovskyi.bigdata201.orchestration.oozie;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Config {

	private static volatile Config INSTANCE;

	private final String defaultFS;
	private final String inputPath;
	private final String outputPath;

	public Config(String defaultFS, String inputPath, String outputPath) {
		this.defaultFS = defaultFS;
		this.inputPath = inputPath;
		this.outputPath = outputPath;
	}

	public String getDefaultFS() {
		return defaultFS;
	}

	public String getInputPath() {
		return inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public static Config parse(String[] args) {

		if (INSTANCE != null) {
			throw new IllegalStateException("config already parsed");
		}

		Set<String> arguments = new HashSet<>(Arrays.asList(args));

		String defaultFS = arguments.stream().filter(arg -> arg.startsWith("--defaultFS=")).findFirst()
				.map(arg -> arg.replace("--defaultFS=", "")).get();
		if (defaultFS == null) {
			throw new IllegalArgumentException("missing required argument: defaultFS");
		}

		String inputPath = arguments.stream().filter(arg -> arg.startsWith("--inputPath=")).findFirst()
				.map(arg -> arg.replace("--inputPath=", "")).get();
		if (inputPath == null) {
			throw new IllegalArgumentException("missing required argument: inputPath");
		}

		String outputPath = arguments.stream().filter(arg -> arg.startsWith("--outputPath=")).findFirst()
				.map(arg -> arg.replace("--outputPath=", "")).get();
		if (outputPath == null) {
			throw new IllegalArgumentException("missing required argument: outputPath");
		}

		INSTANCE = new Config(defaultFS, inputPath, outputPath);
		return INSTANCE;
	}

	public static Config instance() {
		if (INSTANCE == null) {
			throw new IllegalStateException("config is not loaded");
		}
		return INSTANCE;
	}
}

package anatoliisatanovskyi.bigdata201.orchestration;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Config {

	private static volatile Config INSTANCE;

	private final String defaultFS;
	private final String inputPath;
	private final String outputPath;
	private final String outputType;

	public Config(String defaultFS, String inputPath, String outputPath, String outputType) {
		this.defaultFS = defaultFS;
		this.inputPath = inputPath;
		this.outputPath = outputPath;
		this.outputType = outputType;
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

	public String getOutputType() {
		return outputType;
	}

	public static Config parse(String... args) {

		Set<String> arguments = new HashSet<>(Arrays.asList(args));

		String defaultFS = arguments.stream().filter(arg -> arg.startsWith("--defaultFS=")).findFirst()
				.map(arg -> arg.replace("--defaultFS=", "")).orElse(null);
		String inputPath = arguments.stream().filter(arg -> arg.startsWith("--inputPath=")).findFirst()
				.map(arg -> arg.replace("--inputPath=", "")).get();
		String outputPath = arguments.stream().filter(arg -> arg.startsWith("--outputPath=")).findFirst()
				.map(arg -> arg.replace("--outputPath=", "")).get();
		String outputType = arguments.stream().filter(arg -> arg.startsWith("--outputType=")).findFirst()
				.map(arg -> arg.replace("--outputType=", "")).get();

		INSTANCE = new Config(defaultFS, inputPath, outputPath, outputType);
		return INSTANCE;
	}

	public static Config instance() {
		if (INSTANCE == null) {
			throw new IllegalStateException("config is not loaded");
		}
		return INSTANCE;
	}
}

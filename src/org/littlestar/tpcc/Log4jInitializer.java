package org.littlestar.tpcc;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

public class Log4jInitializer {
	//private static final String DEFAULT_LOG_PATTERN = "%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level %c{1.} (%M:%L): %msg%n%throwable";
	private static final String DEFAULT_LOG_PATTERN = "%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level %c{1.}: %msg%n%throwable";
	private final PatternLayout defaultLayout;
	private final Configuration config;
	
	private Log4jInitializer(Level rootLevel) {
		final LoggerContext context = (LoggerContext) LogManager.getContext(false);
		config = context.getConfiguration();
		Map<String, LoggerConfig> loggers = config.getLoggers();
		for (String key : loggers.keySet()) {
			config.removeLogger(key);
		}
		final LoggerConfig rootLoggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
		for (String key : rootLoggerConfig.getAppenders().keySet()) {
			rootLoggerConfig.removeAppender(key);
		}
		rootLoggerConfig.setLevel(rootLevel);
		defaultLayout = PatternLayout.newBuilder().withPattern(DEFAULT_LOG_PATTERN).withCharset(StandardCharsets.UTF_8)
				.build();
	}
	
	public static Log4jInitializer initialize(Level rootLevel) {
		return new Log4jInitializer(rootLevel);
	}
	
	public Log4jInitializer withConsoleAppender(Level level) {
		ConsoleAppender consoleAppender = ConsoleAppender.newBuilder().setName("ConsoleAppender")
				.setLayout(defaultLayout)
				//.withImmediateFlush(true)
				.build();
		consoleAppender.start();
		config.addAppender(consoleAppender);
		config.getRootLogger().addAppender(consoleAppender, level, null);
		return this;
	}
	
	public Log4jInitializer withFileAppender(Level level, String fileName, boolean append) {
		FileAppender fileAppender = FileAppender.newBuilder()
				.setName("FileAppender")
				.setLayout(defaultLayout)
				.withFileName(fileName)
				.withAppend(append)
				//.withBufferedIo(true)
				//.withBufferSize(8192)
				//.setIgnoreExceptions(ignoreExceptions)
				.build();
		fileAppender.start();
		config.addAppender(fileAppender);
		config.getRootLogger().addAppender(fileAppender, level, null);
		return this;
	}
	
	public Log4jInitializer withRollingFileAppender(Level level, String fileName, String fileSize, String maxFilesKeep) {
		SizeBasedTriggeringPolicy policy = SizeBasedTriggeringPolicy.createPolicy(fileSize);
		DefaultRolloverStrategy strategy = DefaultRolloverStrategy.newBuilder().withMax(maxFilesKeep)
				.withFileIndex("min").build();
		RollingFileAppender rollingFileAppender = RollingFileAppender.newBuilder()
				.setName("RollingFileAppender")
				.withAppend(true)
				//.withBufferedIo(true)
                //.withBufferSize(8192)
				//.withLocking(false)
				.setLayout(defaultLayout)
				.withPolicy(policy)
				.withStrategy(strategy)
				.withFilePattern(fileName+".%i")
				.withFileName(fileName).build();
		rollingFileAppender.start();
		config.addAppender(rollingFileAppender);
		config.getRootLogger().addAppender(rollingFileAppender, level, null);
		return this;
	}
	
	public Log4jInitializer withLevel(String loggerName, Level level) {
		Configurator.setLevel(loggerName, level);
		return this;
	}
}

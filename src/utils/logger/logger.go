package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	loggers     = make(map[string]*zap.Logger)
	loggerMutex sync.RWMutex

	// Default config values
	defaultLogDir       = "logs"
	defaultLogToFile    = false
	defaultLogToConsole = true
	defaultLogLevel     = "info"
)

type Config struct {
	LogDir       string
	LogToFile    bool
	LogToConsole bool
	LogLevel     string
}

func Initialize() error {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		return fmt.Errorf("error loading .env file: %w", err)
	}

	// Ensure the logs directory exists
	config := loadConfig()
	if config.LogToFile {
		if err := os.MkdirAll(config.LogDir, 0755); err != nil {
			return fmt.Errorf("error creating log directory: %w", err)
		}
	}
	return nil
}

func loadConfig() Config {
	return Config{
		LogDir:       getEnv("LOG_DIR", defaultLogDir),
		LogToFile:    getEnvBool("LOG_TO_FILE", defaultLogToFile),
		LogToConsole: getEnvBool("LOG_TO_CONSOLE", defaultLogToConsole),
		LogLevel:     getEnv("LOG_LEVEL", defaultLogLevel),
	}
}

// GetLogger returns a logger for the specified package
func GetLogger(packageName string) *zap.Logger {
	loggerMutex.RLock()
	logger, exists := loggers[packageName]
	loggerMutex.RUnlock()

	if !exists {
		logger = createLogger(packageName)
		loggerMutex.Lock()
		loggers[packageName] = logger
		loggerMutex.Unlock()
	}

	return logger
}

// createLogger creates a new logger for a specific package
func createLogger(packageName string) *zap.Logger {
	config := loadConfig()

	// Parse log level
	level := getLogLevel(config.LogLevel)

	// Setup encoder
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	// Create cores based on configuration
	var cores []zapcore.Core

	// Add file core if configured to log to file
	if config.LogToFile {
		logFile := filepath.Join(config.LogDir, packageName+".log")
		writer, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			fileEncoder := zapcore.NewJSONEncoder(encoderConfig)
			cores = append(cores, zapcore.NewCore(fileEncoder, zapcore.AddSync(writer), level))
		}
	}

	// Add console core if configured to log to console
	if config.LogToConsole {
		consoleEncoder := zapcore.NewConsoleEncoder(encoderConfig)
		cores = append(cores, zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level))
	}

	// Create a logger with the configured cores
	core := zapcore.NewTee(cores...)
	return zap.New(core, zap.AddCaller())
}

// Helper functions for environment variables
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value, exists := os.LookupEnv(key); exists {
		return value == "true" || value == "1" || value == "yes"
	}
	return defaultValue
}

// getLogLevel converts a string log level to zapcore.Level
func getLogLevel(levelStr string) zapcore.Level {
	switch levelStr {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

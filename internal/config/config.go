package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all gateway configuration loaded from environment variables.
// Shares the same env var names as Auto-Scholar where applicable.
type Config struct {
	// HTTP Server
	HTTPAddr        string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	MaxUploadSizeMB int64
	ShutdownTimeout time.Duration

	// PostgreSQL
	PostgresHost     string
	PostgresPort     int
	PostgresDB       string
	PostgresUser     string
	PostgresPassword string
	PostgresPoolMax  int
	PostgresPoolMin  int

	// MinIO
	MinIOEndpoint   string
	MinIOAccessKey  string
	MinIOSecretKey  string
	MinIOSecure     bool
	BucketRaw       string
	BucketProcessed string

	// Redis
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// Qdrant
	QdrantHost           string
	QdrantGRPCPort       int
	QdrantCollectionName string

	// Embedding
	EmbeddingModel      string
	EmbeddingDimensions int
	EmbeddingBatchSize  int
	EmbeddingAPIKey     string
	EmbeddingBaseURL    string

	// Chunking
	ChunkSizeTokens    int
	ChunkOverlapTokens int
	TiktokenModel      string

	// Worker Pool
	WorkerCount         int
	DispatchInterval    time.Duration
	HeartbeatInterval   time.Duration
	ZombieCheckInterval time.Duration
	ZombieTimeout       time.Duration
	MaxRetries          int

	// PDF
	PDFDownloadTimeout time.Duration
	PDFMaxSizeMB       int64
}

// Load reads configuration from environment variables with sensible defaults.
func Load() *Config {
	return &Config{
		// HTTP Server
		HTTPAddr:        envStr("HTTP_ADDR", ":8081"),
		ReadTimeout:     envDuration("HTTP_READ_TIMEOUT", 60*time.Second),
		WriteTimeout:    envDuration("HTTP_WRITE_TIMEOUT", 120*time.Second),
		MaxUploadSizeMB: envInt64("MAX_UPLOAD_SIZE_MB", 50),
		ShutdownTimeout: envDuration("SHUTDOWN_TIMEOUT", 15*time.Second),

		// PostgreSQL — same env vars as Auto-Scholar
		PostgresHost:     envStr("POSTGRES_HOST", "localhost"),
		PostgresPort:     envInt("POSTGRES_PORT", 5432),
		PostgresDB:       envStr("POSTGRES_DB", "autoscholar"),
		PostgresUser:     envStr("POSTGRES_USER", "autoscholar"),
		PostgresPassword: envStr("POSTGRES_PASSWORD", "autoscholar"),
		PostgresPoolMax:  envInt("POSTGRES_POOL_MAX", 20),
		PostgresPoolMin:  envInt("POSTGRES_POOL_MIN", 5),

		// MinIO — same env vars as Auto-Scholar
		MinIOEndpoint:   envStr("MINIO_ENDPOINT", "localhost:9000"),
		MinIOAccessKey:  envStr("MINIO_ACCESS_KEY", "minioadmin"),
		MinIOSecretKey:  envStr("MINIO_SECRET_KEY", "minioadmin"),
		MinIOSecure:     envBool("MINIO_SECURE", false),
		BucketRaw:       envStr("MINIO_BUCKET_RAW", "rag-raw"),
		BucketProcessed: envStr("MINIO_BUCKET_PROCESSED", "rag-processed"),

		// Redis — same env vars as Auto-Scholar
		RedisAddr:     fmt.Sprintf("%s:%d", envStr("REDIS_HOST", "localhost"), envInt("REDIS_PORT", 6379)),
		RedisPassword: envStr("REDIS_PASSWORD", ""),
		RedisDB:       envInt("REDIS_DB", 0),

		// Qdrant — same env vars as Auto-Scholar
		QdrantHost:           envStr("QDRANT_HOST", "localhost"),
		QdrantGRPCPort:       envInt("QDRANT_GRPC_PORT", 6334),
		QdrantCollectionName: envStr("QDRANT_COLLECTION_NAME", "paper_chunks"),

		// Embedding — dedicated vars with LLM fallback
		EmbeddingModel:      envStr("EMBEDDING_MODEL", "text-embedding-3-small"),
		EmbeddingDimensions: envInt("EMBEDDING_DIMENSIONS", 1536),
		EmbeddingBatchSize:  envInt("EMBEDDING_BATCH_SIZE", 100),
		EmbeddingAPIKey:     envStrFallback("EMBEDDING_API_KEY", "LLM_API_KEY", ""),
		EmbeddingBaseURL:    envStrFallback("EMBEDDING_BASE_URL", "LLM_BASE_URL", "https://api.openai.com/v1"),

		// Chunking — same constants as Auto-Scholar
		ChunkSizeTokens:    envInt("CHUNK_SIZE_TOKENS", 512),
		ChunkOverlapTokens: envInt("CHUNK_OVERLAP_TOKENS", 50),
		TiktokenModel:      envStr("TIKTOKEN_MODEL", "cl100k_base"),

		// Worker Pool
		WorkerCount:         envInt("WORKER_COUNT", 4),
		DispatchInterval:    envDuration("DISPATCH_INTERVAL", 2*time.Second),
		HeartbeatInterval:   envDuration("HEARTBEAT_INTERVAL", 30*time.Second),
		ZombieCheckInterval: envDuration("ZOMBIE_CHECK_INTERVAL", 30*time.Second),
		ZombieTimeout:       envDuration("ZOMBIE_TIMEOUT", 5*time.Minute),
		MaxRetries:          envInt("MAX_RETRIES", 3),

		// PDF
		PDFDownloadTimeout: envDuration("PDF_DOWNLOAD_TIMEOUT", 30*time.Second),
		PDFMaxSizeMB:       envInt64("PDF_MAX_SIZE_MB", 50),
	}
}

// PostgresDSN returns the PostgreSQL connection string.
func (c *Config) PostgresDSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=disable",
		c.PostgresUser, c.PostgresPassword,
		c.PostgresHost, c.PostgresPort, c.PostgresDB,
	)
}

// MaxUploadBytes returns the max upload size in bytes.
func (c *Config) MaxUploadBytes() int64 {
	return c.MaxUploadSizeMB << 20
}

// PDFMaxSizeBytes returns the max PDF size in bytes.
func (c *Config) PDFMaxSizeBytes() int64 {
	return c.PDFMaxSizeMB << 20
}

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envStrFallback(primary, secondary, fallback string) string {
	if v := os.Getenv(primary); v != "" {
		return v
	}
	if v := os.Getenv(secondary); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envInt64(key string, fallback int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

// Package adapters contains telemetry shared by otherwise isolated benchmark
// adapters. It does not translate queue or delivery semantics.
package adapters

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/experiment"
)

func RedisPoint(ctx context.Context, client *redis.Client, at time.Duration) experiment.RedisPoint {
	info, _ := client.Info(ctx, "memory", "stats", "cpu").Result()
	return experiment.RedisPoint{
		At:              at,
		CPUSeconds:      infoFloat(info, "used_cpu_sys") + infoFloat(info, "used_cpu_user"),
		UsedMemoryBytes: int64(infoFloat(info, "used_memory")),
		Commands:        int64(infoFloat(info, "total_commands_processed")),
		NetInputBytes:   int64(infoFloat(info, "total_net_input_bytes")),
		NetOutputBytes:  int64(infoFloat(info, "total_net_output_bytes")),
	}
}

func infoFloat(info, key string) float64 {
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, key+":") {
			value, _ := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, key+":")), 64)
			return value
		}
	}
	return 0
}

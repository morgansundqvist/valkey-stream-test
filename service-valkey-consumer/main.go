package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"
)

func main() {
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()

	v, _ := client.Do(ctx, client.B().XgroupCreate().Key("test").Group("test-group").Id("$").Build()).ToString()
	if err != nil {
		panic(err)
	}

	fmt.Printf("XGROUP CREATE: %v\n", v)

	uuidString := uuid.New().String()

	v, _ = client.Do(ctx, client.B().XgroupCreateconsumer().Key("test").Group("test-group").Consumer(uuidString).Build()).ToString()

	fmt.Printf("XGROUP CREATECONSUMER: %v\n", v)

	// Channel to listen for interrupt signals (Ctrl+C)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// Goroutine to capture the signal and handle cleanup
	go func() {
		sig := <-signalChan
		fmt.Printf("\nReceived signal: %s. Cleaning up...\n", sig)

		// Remove the consumer from the Redis stream group
		removeConsumer(ctx, &client, uuidString)

		// Exit the application
		os.Exit(0)
	}()

	//infinity loop
	for {

		strSlice, err := client.Do(ctx, client.B().Xreadgroup().Group("test-group", uuidString).Noack().Streams().Key("test").Id(">").Build()).AsXRead()
		if err != nil {
			fmt.Printf("XREADGROUP ERR: %v\n", err)
			continue
		}

		fmt.Printf("XREADGROUP: %v\n", strSlice)
		for _, arrayValue := range strSlice["test"] {
			client.Do(ctx, client.B().Xack().Key("test").Group("test-group").Id(arrayValue.ID).Build())
			go func() {
				client.Do(ctx, client.B().Xdel().Key("test").Id(arrayValue.ID).Build())
			}()
		}
	}
}

// Function to remove the consumer
func removeConsumer(ctx context.Context, client *valkey.Client, uuidString string) {
	clientReal := *client

	v, err := clientReal.Do(ctx, clientReal.B().XgroupDelconsumer().Key("test").Group("test-group").Consumername(uuidString).Build()).ToInt64()

	if err != nil {
		fmt.Printf("Failed to remove consumer: %v\n", err)
		return
	}
	fmt.Printf("XGROUP DELCONSUMER: %v\n", v)
}

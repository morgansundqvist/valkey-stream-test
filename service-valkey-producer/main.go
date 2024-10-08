package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/valkey-io/valkey-go"
)

func main() {
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()

	//for 1 000 000 values add the number to the Redis Stream with the key "test"

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// Goroutine to capture the signal and handle cleanup
	go func() {
		sig := <-signalChan
		fmt.Printf("\nReceived signal: %s. Cleaning up...\n", sig)

		// Remove the consumer from the Redis stream group
		removeStream(ctx, &client, "test")

		// Exit the application
		os.Exit(0)
	}()

	dataToSave := map[string]string{
		"number": "1",
		"value":  "one",
		"super":  "cool",
	}

	strJson, err := json.Marshal(dataToSave)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 100000000000000; i++ {
		stringRepInt := strconv.Itoa(i)
		client.Do(ctx, client.B().Xadd().Key("test").Id("*").FieldValue().FieldValue("number", stringRepInt).FieldValue("data", string(strJson)).Build())
		//every 100 values print the value
		if i%100 == 0 {
			fmt.Printf("Value: %d\n", i)
		}
	}

}

func removeStream(ctx context.Context, client *valkey.Client, s string) {
	realClient := *client

	realClient.Do(ctx, realClient.B().Del().Key(s).Build())
}

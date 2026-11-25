// A single-in-flight producer for the matched-client SSE benchmark. The driver
// starts its clock before signaling the producer and checks every SSE payload.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func post(client *http.Client, body []byte) error {
	response, err := client.Post("http://spool:7474/topics/status/records", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	var receipt struct{ Offset int64 }
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP status: %s", response.Status)
	}
	if err := json.NewDecoder(response.Body).Decode(&receipt); err != nil {
		return err
	}
	if receipt.Offset <= 0 {
		return fmt.Errorf("invalid offset: %d", receipt.Offset)
	}
	_, err = io.Copy(io.Discard, response.Body)
	return err
}

func send(ch *amqp.Channel, event map[string]any, body []byte) error {
	task := event["task_id"].(string)
	message := amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent,
		CorrelationId: event["correlation_id"].(string),
		Headers:       amqp.Table{"task_id": task, "status": event["status"].(string)},
		Body:          body,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	confirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, "fanout.status.ex", task, true, false, message)
	if err != nil {
		return err
	}
	ack, err := confirm.WaitContext(ctx)
	if err == nil && !ack {
		return fmt.Errorf("negative AMQP confirmation")
	}
	return err
}

func run(arm string) error {
	input := bufio.NewScanner(os.Stdin)
	input.Buffer(make([]byte, 65536), 128<<20)
	if !input.Scan() {
		return fmt.Errorf("missing event list")
	}
	var events []map[string]any
	if err := json.Unmarshal(input.Bytes(), &events); err != nil {
		return err
	}
	client := &http.Client{Timeout: 15 * time.Second}
	defer client.CloseIdleConnections()
	var ch *amqp.Channel
	if arm == "rabbit-redis" {
		conn, err := amqp.Dial("amqp://bench:bench@rabbit:5672/")
		if err != nil {
			return err
		}
		defer conn.Close()
		ch, err = conn.Channel()
		if err != nil {
			return err
		}
		defer ch.Close()
		if err := ch.Confirm(false); err != nil {
			return err
		}
	} else if arm != "spooltail" {
		return fmt.Errorf("unknown arm %q", arm)
	}
	fmt.Println("ready")
	for input.Scan() {
		n, err := strconv.Atoi(input.Text())
		if err != nil || n < 0 || n >= len(events) {
			return fmt.Errorf("invalid event number: %s", input.Text())
		}
		event := events[n]
		body, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if ch == nil {
			err = post(client, body)
		} else {
			err = send(ch, event, body)
		}
		if err != nil {
			return err
		}
		fmt.Println("ack")
	}
	return input.Err()
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "expected spooltail or rabbit-redis")
		os.Exit(2)
	}
	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

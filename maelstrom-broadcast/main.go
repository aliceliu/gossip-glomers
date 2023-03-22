package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"golang.org/x/exp/maps"
)

func main() {
	msgs := make(map[float64]bool)
	msgsMutex := sync.RWMutex{}
	topology := make(map[string]interface{})
	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		dest_node := msg.Dest

		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := body["message"].(float64)

		msgsMutex.RLock()
		_, seen_msg := msgs[message]
		msgsMutex.RUnlock()

		if !seen_msg {
			msgsMutex.Lock()
			msgs[message] = true
			msgsMutex.Unlock()
			neighbors, ok := topology[dest_node]
			if ok {
				for _, neighbor := range neighbors.([]interface{}) {
					n.Send(neighbor.(string), body)
				}
			}
		}

		body["type"] = "broadcast_ok"
		delete(body, "message")
		return n.Reply(msg, body)
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "read_ok"
		msgsMutex.RLock()
		body["messages"] = maps.Keys(msgs)
		msgsMutex.RUnlock()

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "topology_ok"
		topology = body["topology"].(map[string]interface{})
		delete(body, "topology")

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

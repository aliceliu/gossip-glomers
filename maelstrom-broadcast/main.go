package main

import (
	"encoding/json"
	"log"
	"math"
	"sync"
	"time"

	guuid "github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"golang.org/x/exp/maps"
)

type unackedMsg struct {
	dest string
	body map[string]any
}

type forwardMsg struct {
	dest string
	body map[string]any
	isAck bool
	attempt int
}

func main() {
	msgs := make(map[float64]bool)
	msgsMutex := sync.RWMutex{}
	topology := make(map[string]interface{})
	n := maelstrom.NewNode()
	unackedMsgs := make(map[string]map[string]unackedMsg)

	forwardCh := make(chan forwardMsg)
	sendRetry := func(msg forwardMsg) {
		delay := math.Min(math.Pow(2, float64(msg.attempt - 1)), 5)
		time.Sleep(time.Duration(delay) * time.Second)
		forwardCh <- msg
	}
	handleForward := func() {
		for {
			fwdMsg := <- forwardCh
			neighbor := fwdMsg.dest
			uuid := fwdMsg.body["uuid"].(string)
			body := fwdMsg.body

			if fwdMsg.isAck {
				val, ok := unackedMsgs[neighbor]
				if ok {
					delete(val, uuid)
				}
			} else {
				if fwdMsg.attempt == 1 {
					val, ok := unackedMsgs[neighbor]
					if ok {
						val[uuid] = unackedMsg{neighbor, body}
					} else {
						unackedMsgs[neighbor] = map[string]unackedMsg{}
						unackedMsgs[neighbor][uuid] = unackedMsg{neighbor, body}
					}
					n.RPC(neighbor, body, func(msg maelstrom.Message) error {
						forwardCh <- forwardMsg{
								dest:    neighbor,
								body:    body,
								isAck:   true,
								attempt: 0,
							}
						return nil
					})
					go sendRetry(forwardMsg{
						dest:    neighbor,
						body:    body,
						isAck:   false,
						attempt: fwdMsg.attempt + 1,
					})
				} else {
					val, ok := unackedMsgs[neighbor]
					if ok {
						_, ok := val[uuid]
						if ok {
							n.RPC(neighbor, body, func(msg maelstrom.Message) error {
								forwardCh <- forwardMsg{
										dest:    neighbor,
										body:    body,
										isAck:   true,
										attempt: fwdMsg.attempt + 1,
									}
								return nil
							})
							go sendRetry(fwdMsg)
						}
					}
				}
			}
		}
	}
	go handleForward()

	broadcastMessage := func(msg maelstrom.Message) error {
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
	
			var forwardBody map[string]any
			if err := json.Unmarshal(msg.Body, &forwardBody); err != nil {
				return err
			}
			forwardBody["type"] = "forward"
			uuid, ok := forwardBody["uuid"]
			if !ok {
				uuid = guuid.New().String()
			}
			forwardBody["uuid"] = uuid
	
			neighbors, ok := topology[msg.Dest]
			if ok {
				for _, neighbor_node := range neighbors.([]interface{}) {
					forwardCh <- forwardMsg{
						dest:    neighbor_node.(string),
						body:    forwardBody,
						isAck:   false,
						attempt: 1,
					}
				}
			}
		}
	
		body["type"] = "broadcast_ok"
		delete(body, "message")
		return n.Reply(msg, body)
	}
	

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		return broadcastMessage(msg)
	})
	n.Handle("forward", func(msg maelstrom.Message) error {
		return broadcastMessage(msg)
	})
	n.Handle("forward_ok", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		forwardCh <- forwardMsg{
			dest:    msg.Src,
			body:    body,
			isAck:   true,
			attempt: 0,
		}
		return nil
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

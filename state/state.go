package state

import "github.com/CefBoud/monkafka/types"

var ConsumerState = make(map[string]uint64)
var TopicStateInstance types.TopicsState = make(types.TopicsState)
var LogDir string

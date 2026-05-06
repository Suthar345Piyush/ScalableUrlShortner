// snowflake unique int64 id generation, where each of the instance will have own unique  node id according to the env

package idgen

import (
	"fmt"

	"github.com/bwmarrin/snowflake"
)

// the service layer will depand on this generator  interface

type Generator interface {
	Next() int64
}

// snowflake id struct

type snowflakeGen struct {
	node *snowflake.Node
}

// the snowflake id is unique for all, and range is between 0 - 1023

func New(nodeID int64) (Generator, error) {

	node, err := snowflake.NewNode(nodeID)

	if err != nil {
		return nil, fmt.Errorf("idgen: failed to create snowflake node %d: %w", nodeID, err)
	}

	return &snowflakeGen{node: node}, nil

}

func (g *snowflakeGen) Next() int64 {
	return g.node.Generate().Int64()
}

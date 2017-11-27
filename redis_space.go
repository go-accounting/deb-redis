package debredis

import (
	"bytes"
	"encoding/gob"
	"go-accounting/deb"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

type redisBlock deb.DataBlock

func NewRedisSpace(master string, addrs []string, prefix string) (deb.Space, error) {
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		MasterName: master,
		Addrs:      addrs,
	})
	if err := client.Ping().Err(); err != nil {
		return nil, errors.Wrap(err, "ping failed")
	}
	var ls *deb.LargeSpace
	key := "blocks"
	if prefix != "" {
		key = prefix + "/" + key
	}
	errch := make(chan error, 1)
	in := func() chan *deb.DataBlock {
		c := make(chan *deb.DataBlock)
		go func() {
			var err error
			defer func() {
				close(c)
				errch <- err
			}()
			// send all blocks by c channel
			var l int64
			l, err = client.LLen(key).Result()
			for i := int64(0); i < l; i++ {
				if err != nil {
					err = errors.Wrap(err, "read block failed")
					break
				}
				block := ls.NewDataBlock()
				err = client.LIndex(key, i).Scan((*redisBlock)(block))
				c <- block
			}
		}()
		return c
	}
	out := make(chan []*deb.DataBlock)
	go func() {
		for blocks := range out {
			var err error
			for _, block := range blocks {
				// persist block, if block.Key is nil then is a new block
				if block.Key == nil {
					block.Key, err = client.LLen(key).Result()
					if err != nil {
						break
					}
					err = client.RPush(key, (*redisBlock)(block)).Err()
				} else {
					err = client.LSet(key, block.Key.(int64), (*redisBlock)(block)).Err()
				}
				if err != nil {
					err = errors.Wrap(err, "write block failed")
					break
				}
			}
			errch <- err
		}
	}()
	ls = deb.NewLargeSpace(1014*1024, in, out, errch)
	return ls, nil
}

func (rb *redisBlock) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode((*deb.DataBlock)(rb)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (rb *redisBlock) UnmarshalBinary(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode((*deb.DataBlock)(rb)); err != nil {
		return err
	}
	return nil
}

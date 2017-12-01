package debredis

import (
	"bytes"
	"encoding/gob"

	"github.com/go-accounting/deb"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

type redisBlock deb.DataBlock

func NewRedisSpace(master string, addrs []string, prefix1, prefix2 *string) (deb.Space, error) {
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		MasterName: master,
		Addrs:      addrs,
	})
	if err := client.Ping().Err(); err != nil {
		return nil, errors.Wrap(err, "ping failed")
	}
	var ls *deb.LargeSpace
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
			l, err = client.LLen(key(prefix1, prefix2)).Result()
			for i := int64(0); i < l; i++ {
				if err != nil {
					err = errors.Wrap(err, "read block failed")
					break
				}
				block := ls.NewDataBlock()
				err = client.LIndex(key(prefix1, prefix2), i).Scan((*redisBlock)(block))
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
					block.Key, err = client.LLen(key(prefix1, prefix2)).Result()
					if err != nil {
						break
					}
					err = client.RPush(key(prefix1, prefix2), (*redisBlock)(block)).Err()
				} else {
					err = client.LSet(key(prefix1, prefix2), block.Key.(int64), (*redisBlock)(block)).Err()
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

func key(s1, s2 *string) string {
	if (s1 == nil || *s1 == "") && (s2 == nil || *s2 == "") {
		return "blocks"
	} else if s1 == nil || *s1 == "" {
		return *s2 + "/blocks"
	} else if s2 == nil || *s2 == "" {
		return *s1 + "/blocks"
	}
	return *s1 + "/" + *s2 + "/blocks"
}

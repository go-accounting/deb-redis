package main

import debredis "github.com/go-accounting/deb-redis"

func NewSpace(config map[string]interface{}, ss ...*string) (interface{}, error) {
	v := config["NewSpace/Addresses"].([]interface{})
	addrs := make([]string, len(v))
	for i, a := range v {
		addrs[i] = a.(string)
	}
	master, _ := config["Master"].(string)
	return debredis.NewRedisSpace(master, addrs, ss[0], ss[1])
}

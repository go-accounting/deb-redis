package main

import debredis "github.com/go-accounting/deb-redis"

func NewSpace(settings map[string]interface{}, p1 *string, p2 *string) (interface{}, error) {
	v := settings["Addresses"].([]interface{})
	addrs := make([]string, len(v))
	for i, a := range v {
		addrs[i] = a.(string)
	}
	master, _ := settings["Master"].(string)
	return debredis.NewRedisSpace(master, addrs, p1, p2)
}

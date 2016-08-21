package main

import (
	crand "crypto/rand"
	"fmt"
	"time"

	cuckoo "github.com/jakobvarmose/cuckoo/core"
)

func main() {
	c, err := cuckoo.CreateCuckoo("pins", 17)
	if err != nil {
		panic(err)
	}
	defer c.Close()
	key := make([]byte, 34)
	t := time.Now()
	fmt.Println(t)
	N := 1000000
	keys := make([]string, N)
	for i := 0; i < len(keys); i++ {
		//if rand.Intn(10) < 9 {
		crand.Read(key)
		//}
		keys[i] = string(key)
	}
	keys2 := make([]string, N)
	for i := 0; i < len(keys); i++ {
		keys2[i] = string(key)
	}
	for i := 0; i < len(keys); i++ {
		crand.Read(key)
		keys[i] = string(key)
	}
	fmt.Println(time.Now().Sub(t))
	t = time.Now()
	for i := 0; i < len(keys); i++ {
		err := c.Increment(keys[i])
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}
	c.Sync()
	fmt.Println(time.Now().Sub(t))
	t = time.Now()
	for i := 0; i < len(keys2); i++ {
		count, err := c.Get(keys2[i])
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		if count != 0 {
			fmt.Println("Claims to have unstored key")
			return
		}
		count, err = c.Get(keys[i])
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		if count != 1 {
			fmt.Println("Does not have stored key")
			return
		}
	}
	c.Sync()
	fmt.Println(time.Now().Sub(t))
	t = time.Now()
	for i := 0; i < len(keys); i++ {
		err := c.Decrement(keys[i])
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}
	c.Sync()
	u := time.Now().Sub(t)
	fmt.Println(u)
}

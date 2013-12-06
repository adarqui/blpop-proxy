package main

import (
	"fmt"
	"log"
	"menteslibres.net/gosexy/redis"
	"os"
	"strconv"
	"strings"
	"time"
)

/*
 * Some structs for the redis connections
 */

type Redis_Pop struct {
	Key   string
	Value string
}

type Redis_Params struct {
	Host string
	Port uint
}

type Router struct {
	In_Params *Redis_Params
	In        *redis.Client

	Out_Params *Redis_Params
	Out        *redis.Client

	Log_Lost *os.File
}

func usage() {
	log.Fatal("usage: ./blproxy <in_redis:port> <out_redis:port>")
}

func log_lost_init() *os.File {
	f, err := os.OpenFile("blpop-proxy.lost.log", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		log.Fatal("log_lost_init:OpenFile:blpop-proxy.lost.log:Err:", err)
	}
	return f
}

func redis_parse_host_port(Arg string, RP *Redis_Params) {
	in_arr := strings.Split(Arg, ":")

	RP.Host = in_arr[0]
	port, err := strconv.Atoi(in_arr[1])
	if err != nil {
		redis_error("redis_parse_host_port:strconv:Err:", err)
	}

	RP.Port = uint(port)
}

func redis_init() *Router {

	R := new(Router)
	R.In = redis.New()
	R.In_Params = new(Redis_Params)
	R.Out = redis.New()
	R.Out_Params = new(Redis_Params)

	redis_parse_host_port(os.Args[1], R.In_Params)
	redis_parse_host_port(os.Args[2], R.Out_Params)

	return R
}

func (R *Router) redis_fini() {
	R.In.Quit()
	R.Out.Quit()
	log.Println("Finished.")
}

func redis_error(S string, E error) {
	log.Fatal(S, E)
}

func (R *Router) redis_connect_in() error {

	err := R.In.ConnectNonBlock(R.In_Params.Host, R.In_Params.Port)
	if err != nil {
		log.Printf("redis_connect:In:Err:%q", err)
		return err
	}
	log.Printf("redis_connect:In:Connected to %v\n", R.In_Params)

	return nil
}

func (R *Router) redis_connect_out() error {

	err := R.Out.Connect(R.Out_Params.Host, R.Out_Params.Port)
	if err != nil {
		log.Printf("redis_connect:Out:Err:%q", err)
		return err
	}
	log.Printf("redis_connect:Out:Connected to %v\n", R.Out_Params)

	return nil
}

func (R *Router) redis_pop_in(C chan *Redis_Pop, Keys []string) {
	for {
		res, err := R.In.BLPop(5, Keys...)
		if err != nil {
			log.Printf("redis_pop_in:BLPop:Err:%q\n", err)
			time.Sleep(1 * time.Second)
			R.redis_connect_in()
			continue
		}

		if len(res) == 0 {
			continue
		}

		pop := Redis_Pop{res[0], res[1]}
		C <- &pop
	}
}

func (R *Router) redis_push_out(C chan *Redis_Pop) {
	var logged bool
	for {
		message := <-C
		for {
			/* Reset logging in case our connection drops again */
			logged = false
			/* try this operation over and over again until we are successful */
			_, err := R.Out.RPush(message.Key, message.Value)
			if err != nil {
				/* save locally, try reconnecting */
				if logged == false {
					R.Log_Lost.WriteString(fmt.Sprintf("%q\n", message))
					logged = true
				}
				for {
					/* Reconnect loop */
					err := R.redis_connect_out()
					if err != nil {
						time.Sleep(1 * time.Second)
						continue
					} else {
						break
					}
				}
			} else {
				break
			}
		}

	}
}

func main() {

	if len(os.Args) < 4 {
		usage()
	}

	keys := os.Args[3:len(os.Args)]

	log.Printf("Initializing blpop-proxy with the following keys: %v\n", keys)

	R := redis_init()

	R.Log_Lost = log_lost_init()

	/* Limit of 1 to ensure we don't pop jobs when "out" is disconnected */
	C := make(chan *Redis_Pop, 1)
	go R.redis_pop_in(C, keys)
	go R.redis_push_out(C)

	select {}
}

package main

import (
	"log"
	"os"
	"strings"
	"strconv"
	"menteslibres.net/gosexy/redis"
)

/*
 * Some structs for the redis connections
 */

type Redis_Pop struct {
	Key string
	Value string
}

type Redis_Params struct {
	Host string
	Port uint
}

type Router struct {
	In_Params *Redis_Params
	In *redis.Client

	Out_Params *Redis_Params
	Out *redis.Client
}

func usage() {
	log.Fatal("usage: ./blproxy <in_redis:port> <out_redis:port>")
}

func redis_parse_host_port(Arg string, RP *Redis_Params) {
	in_arr := strings.Split(Arg,":")

	RP.Host = in_arr[0]
	port,err := strconv.Atoi(in_arr[1])
	if err != nil {
		redis_error("redis_parse_host_port:strconv:Err:",err)
	}

	RP.Port = uint(port)
}

func redis_init() (*Router) {

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
	log.Fatal(S,E)
}

func (R *Router) redis_connect() (error) {

	err := R.In.ConnectNonBlock(R.In_Params.Host, R.In_Params.Port)
	if err != nil {
		redis_error("redis_connect:In:Err:",err)
	}
	log.Printf("redis_connect:In:Connected to %v\n", R.In_Params)

	err = R.Out.ConnectNonBlock(R.Out_Params.Host, R.Out_Params.Port)
	if err != nil {
		redis_error("redis_connect:Out:Err",err)
	}
	log.Printf("redis_connect:Out:Connected to %v\n", R.Out_Params)

	return nil
}

func (R *Router) redis_pop_in(C chan *Redis_Pop, Keys []string) {
	for {
		res,err := R.In.BLPop(300, Keys...)
		if err != nil {
			log.Printf("redis_pop_in:BLPop:Err:%q\n",err)
		}
		pop := Redis_Pop{res[0],res[1]}
		C <- &pop
	}
}

func (R *Router) redis_push_out(C chan *Redis_Pop) {
	for {
		message := <-C
		R.Out.RPush(message.Key, message.Value)
	}
}


func main() {

	if len(os.Args) < 4 {
		usage()
	}

	keys := os.Args[3:len(os.Args)]

	log.Printf("Initializing blpop-proxy with the following keys: %v\n", keys)

	R := redis_init()
	err := R.redis_connect()
	if err != nil {
		redis_error("main:redis_connect:Err:",err)
	}

	C := make(chan *Redis_Pop)
	go R.redis_pop_in(C, keys)
	go R.redis_push_out(C)

	select { }
}

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	getCommand := flag.Bool("get", false, "--get")
	setCommand := flag.Bool("set", false, "--set")

	flag.Parse()

	// Get Command
	if *getCommand {
		data, err := db_get(flag.Arg(0))

		if err != nil {
			fmt.Println("Error in get", err.Error())
			return
		}

		fmt.Println("DATA: ", string(data))

		return
	}

	// Set command
	if *setCommand {
		err := db_set(flag.Arg(0), flag.Arg(1))

		if err != nil {
			fmt.Println("Error in set", err.Error())
			return
		}
	}

}

func db_set(key string, value string) error {

	if len(key) == 0 || len(value) == 0 {
		return errors.New("invalid get request")
	}

	fs, err := os.OpenFile("./database", os.O_WRONLY|os.O_APPEND, 0666)
	defer fs.Close()

	if err != nil {
		return errors.New("error opening file")
	}

	_, err = fs.Write([]byte(key + "," + value + "\n"))

	if err != nil {
		return errors.New("error writing to file")
	}

	return nil
}

func db_get(key string) ([]byte, error) {

	if len(key) == 0 {
		return nil, errors.New("empty get request")
	}

	fs, err := os.OpenFile("./database", os.O_RDONLY, 0444)
	defer fs.Close()

	if err != nil {
		return nil, errors.New("error opening file")
	}

	fileInfo, _ := fs.Stat()

	buffer := make([]byte, fileInfo.Size(), fileInfo.Size())

	// ineffecient reading the whole file but allows us to reverse search
	_, err = fs.Read(buffer)

	if err != nil {
		return nil, errors.New("error reading from file")
	}

	records := strings.Split(string(buffer), "\n")
	var data = make([]string, 2)

	for i := len(records) - 1; i > 0; i-- {
		data = strings.Split(records[i], ",")

		if len(data) != 2 || data[0] != key {
			continue
		}

		return []byte(data[1]), nil
	}

	return nil, nil
}

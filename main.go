package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"godb/internal/engine"
	"godb/internal/index"
)

type function string

const (
	defaultFilename     = "./store"
	defaultMetaFilename = "./store.meta"

	read   function = "read"
	write  function = "write"
	delete function = "delete"
)

func init() {
	err := createFileIfNotExist(defaultFilename, defaultMetaFilename)
	if err != nil {
		panic(err)
	}
}

func main() {
	go func() {
		err := startConsole()
		if err != nil {
			fmt.Printf("quit due to error: %v", err)
			os.Exit(0)
		}
	}()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGINT)
	<-signalCh
	signal.Stop(signalCh)
	fmt.Println("bye~")
	os.Exit(0)
}

func startConsole() error {
	scanner := bufio.NewScanner(os.Stdin)

	var funcName string
	var key string
	var value string

	var coreErr error

	sst := index.NewSST(defaultMetaFilename)
	engine := engine.NewStore(defaultFilename, sst)

	for {
		if !scanner.Scan() {
			break
		}
		input := strings.Split(scanner.Text(), " ")
		if len(input) < 3 {
			if input[0] == string(read) || input[0] == string(delete) {
				funcName = input[0]
				key = input[1]
			} else {
				coreErr = fmt.Errorf("3 arguments required, function mae, key, value, missing some of them")
				break
			}
		} else {
			funcName = input[0]
			key = input[1]
			value = input[2]
		}

		switch funcName {
		case string(write):
			n, err := engine.StoreKV(key, value)
			if err != nil {
				coreErr = err
			}
			fmt.Printf("wrote %d byte data\n", n)
		case string(read):
			val, err := engine.ReadKey(key)
			if err != nil {
				coreErr = err
			}
			fmt.Printf("last found: %s \n", val)
		case string(delete):
			err := engine.DeleteKey(key)
			if err != nil {
				coreErr = err
			}
			fmt.Println("deleted key")
		default:
			coreErr = fmt.Errorf("wrong function name, currently support read or write")
		}

		if coreErr != nil {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	if coreErr != nil {
		return coreErr
	}

	return nil
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

func createFileIfNotExist(filenames ...string) error {
	for _, fn := range filenames {
		if !fileExists(fn) {
			file, err := os.Create(fn)
			if err != nil {
				return err
			}
			defer file.Close()
		}
	}
	return nil
}

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

const (
	defaultFilename     = "./store"
	defaultMetaFilename = "./store.meta"

	read  function = "read"
	write function = "write"
)

func init() {
	err := createFileIfNotExist(defaultFilename, defaultMetaFilename)
	if err != nil {
		panic(err)
	}
}

type function string

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

	for {

		if !scanner.Scan() {
			break
		}
		input := strings.Split(scanner.Text(), " ")
		if len(input) < 3 {
			if input[0] == string(read) {
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
			n, err := storeKV(key, value)
			if err != nil {
				coreErr = err
			}
			fmt.Printf("wrote %d byte data\n", n)
			err = makeMeta(key, len(value))
			if err != nil {
				coreErr = err
			}
		case string(read):
			val, cnt, err := readKey(key)
			if err != nil {
				coreErr = err
			}
			fmt.Printf("read %d data, last found: %s \n", cnt, val)
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

func storeKV(key string, value string) (int, error) {
	file, err := os.OpenFile(defaultFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return -1, err
	}
	defer file.Close()

	data := fmt.Sprintf("%s : %s,\n", key, value)

	n, err := file.WriteString(data)
	if err != nil {
		return -1, err
	}
	return n, nil
}

func readKey(key string) (string, int, error) {
	store, err := os.Open(defaultFilename)
	if err != nil {
		return "", 0, err
	}
	defer store.Close()
	scanner := bufio.NewScanner(store)

	var found bool
	var foundVal string
	var foundCount int

	for scanner.Scan() {
		line := scanner.Text()
		split := strings.Split(line, ":")
		curKey := strings.TrimSpace(split[0])
		curVal := strings.TrimSpace(split[1])
		if curKey == key {
			found = true
			foundVal = curVal
			foundCount++
		}
	}

	if !found {
		return "", 0, fmt.Errorf("no record found")
	}

	return foundVal, foundCount, nil
}

type indexMeta struct {
	Key    string `json:"key"`
	Offset int    `json:"offset"`
}

func makeMeta(key string, valLen int) error {
	lastOffset, err := getLastOffset()
	if err != nil {
		return err
	}

	indexMeta := &indexMeta{
		Key:    key,
		Offset: lastOffset + valLen,
	}
	jsonData, err := json.MarshalIndent(indexMeta, "", "    ")
	if err != nil {
		return err
	}

	file, err := os.OpenFile(defaultMetaFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	_, err = file.Write(append(jsonData, []byte("\n")...))
	if err != nil {
		file.Close()
		return err
	}

	return nil
}

func getLastOffset() (int, error) {
	indexMetaList, err := readIndexMeta()
	if err != nil {
		return -1, err
	}
	if len(indexMetaList) == 0 {
		return 0, nil
	}

	lastIndexMeta := indexMetaList[len(indexMetaList)-1]
	return lastIndexMeta.Offset, nil
}

func readIndexMeta() ([]indexMeta, error) {
	file, err := os.Open(defaultMetaFilename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var indexMetaList []indexMeta

	decoder := json.NewDecoder(file)
	for decoder.More() {
		var indexMeta indexMeta
		if err := decoder.Decode(&indexMeta); err != nil {
			return nil, err
		}
		indexMetaList = append(indexMetaList, indexMeta)
	}
	return indexMetaList, nil
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

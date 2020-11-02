package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/streadway/amqp"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var watcher *fsnotify.Watcher

type Conf struct {
	Path        string `yaml:"path"`
	CopyTo      string `yaml:"copy_to"`
	Extension   string `yaml:"extension"`
	BrokerTopic string `yaml:"brokerTopic"`
	Broker      string `yaml:"broker"`
}

func readConf(filename string) (*Conf, error) {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	c := &Conf{}
	err = yaml.Unmarshal(buf, c)
	if err != nil {
		return nil, fmt.Errorf("in file %q: %v", filename, err)
	}

	return c, nil
}

func watchDir(path string, fi os.FileInfo, err error) error {
	if fi.Mode().IsDir() {
		return watcher.Add(path)
	}

	return nil
}

func copy(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}

func GetConf() *Conf {
	configFile := flag.String("c", "./config.yaml", "config file")
	flag.Parse()

	c, err := readConf(*configFile)
	if err != nil {
		log.Fatal(err)
	}

	return c
}

func main() {
	config := GetConf()
	watcher, _ = fsnotify.NewWatcher()
	defer watcher.Close()

	if err := filepath.Walk(config.Path, watchDir); err != nil {
		fmt.Println("ERROR", err)
	}
	done := make(chan bool)

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				processEvent(event, *config)
			case err := <-watcher.Errors:
				fmt.Println("ERROR", err)
			}
		}
	}()
	<-done
}

func emitEvent(fileName string, conf Conf) {
	fmt.Println("Event on file", fileName)
	message := map[string]interface{}{
		"fileName": fileName,
	}
	bytesRepresentation, err := json.Marshal(message)
	if err != nil {
		log.Println(err)
	} else {
		emmitToRabbit(conf, bytesRepresentation)
	}
}

func emmitToRabbit(conf Conf, bytesRepresentation []byte) {
	conn, err := amqp.Dial(conf.Broker)
	defer conn.Close()

	if err != nil {
		log.Fatalf("%s: %s", "Failed to connect to RabbitMQ", err)
	} else {
		ch, err := conn.Channel()
		defer ch.Close()

		if err != nil {
			log.Fatalf("%s: %s", "Failed to open a channel", err)
		} else {
			q, err := ch.QueueDeclare(
				conf.BrokerTopic,
				true,
				false,
				false,
				false,
				nil,
			)
			if err != nil {
				log.Fatalf("%s: %s", "Failed to declare a queue", err)
			} else {
				err = ch.Publish(
					"",
					q.Name, // routing key
					false,
					false,
					amqp.Publishing{
						ContentType: "application/json",
						Body:        bytesRepresentation,
					})
				if err != nil {
					log.Fatalf("%s: %s", "Failed to publish a message", err)
				} else {
					log.Printf(" [x] Sent %s", bytesRepresentation)
				}
			}
		}
	}
}

func processEvent(event fsnotify.Event, conf Conf) {
	filePath := event.Name
	fileName := filepath.Base(filePath)
	switch op := event.Op.String(); op {
	case "CREATE":
		if strings.HasSuffix(filePath, conf.Extension) {
			bytes, _ := copy(event.Name, conf.CopyTo+fileName)
			if bytes > 0 {
				emitEvent(fileName, conf)
			}
		}
	default:
		fmt.Println("Unhandled event: ", op, " on file: ", fileName)
	}
}

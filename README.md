## Playing with Go and file system watchers.

Let me explain the idea. I want to emit one RabbitMQ message each time new file is generated in a folder. The problem is that I cannot modify the code of the software that generate the files. The idea is generate a filesystem watcher that emits the message. Let's start.

I'm not a Go expert, but Go it's cool for those kind of task. You can can create a executable file and just copy in the desired server and that's all. Just works.

Also there's a fsnotify [package](github.com/fsnotify/fsnotify) to listen filesystem events. I've used fsnotify in the past with PHP and Python.

```go
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
```

And basically that's all. We only need to take care with filesystem events. Fsnotify is very low level and when we use it we realized how programs write files. Some of them create a temp file, then it writes it and finally it renames the file. Sometimes the program creates an empty file and finally writes the file. Basically it's the same but the events are different. In my example I only listen to "CREATE" just enough for my test.

Emit event to RabbitMQ it's also simple. Well documented within documentation.

```go
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
```

In this example I also want to use a YAML file to store configuration. Just for learn how to read YAML files in go.

```go
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
```

And that's all. My binary executable is ready.
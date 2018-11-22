package main

import(
	"bytes"
	"io/ioutil"
	"path/filepath"
	"encoding/json"
	ucl "github.com/nahanni/go-ucl"
)

type Config struct {

	Broker string		`json:"broker.address"`
	Topic string		`json:"topic"`
	Partition string	`json:"partition"`
	Middleware string	`json:"middleware"`

	SASL struct {
		Username string	`json:"username"`
		Password string	`json:"password"`
	} `json:"sasl,omitempty"`

}

func (c *Config) FromFile(fname string) error {
	var (
		file, uclJSON	[]byte
		err		error
		fileBytes	*bytes.Buffer
		parser		*ucl.Parser
		uclData		map[string]interface{}
	)

	if fname, err = filepath.Abs(fname); err != nil {
		return err
	}

	if fname, err = filepath.EvalSymlinks(fname); err != nil {
		return err
	}

	if file, err = ioutil.ReadFile(fname); err != nil {
		return err
	}

	fileBytes = bytes.NewBuffer(file)
	parser = ucl.NewParser(fileBytes)

	if uclData, err = parser.Ucl(); err != nil {
		return err
	}

	if uclJSON, err = json.Marshal(uclData); err != nil {
		return err
	}

	return json.Unmarshal(uclJSON, &c)
}

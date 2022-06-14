package smoldb

import (
	"encoding/json"
	"errors"
	"github.com/narvikd/filekit"
	"log"
	"sync"
	"time"
)

const filePath = "db.json"

var (
	once  sync.Once
	DB    *Collection
	debug = false
)

func New() (*Collection, error) {
	d, err := newDB()
	if err != nil {
		return nil, err
	}
	return d, nil
}

func NewDebug() (*Collection, error) {
	debug = true
	d, err := newDB()
	if err != nil {
		return nil, err
	}
	return d, nil
}

func newDB() (*Collection, error) {
	if DB != nil {
		return nil, errors.New("DB already initialized")
	}
	var errSetSingleton error

	once.Do(func() {
		err := setSingleton()
		if err != nil {
			errSetSingleton = err
		} else {
			go DB.watcher()
		}
	})

	if errSetSingleton != nil {
		return nil, errSetSingleton
	}

	return DB, nil
}

func setSingleton() error {
	collection, err := newCollection()
	if err != nil {
		return err
	}
	DB = collection
	return nil
}

func (c *Collection) watcher() {
	for {
		if len(c.Records) > 0 {
			if c.LastRecordsHash == "" { // Is not set
				c.saveRecordsAndSetHash() // Set it and save the records file
			} else { // If it is set
				if c.getHash() != c.LastRecordsHash { // But a new hash isn't the same as the stored one
					c.saveRecordsAndSetHash() // Set it and save the records file
				}
			}
		}
		time.Sleep(15 * time.Second)
	}
}

func (c *Collection) saveRecordsAndSetHash() {
	err := c.saveRecordsToFile()
	if err != nil {
		log.Println("DB: there was a problem saving the records file:", err)
	}
	c.LastRecordsHash = c.getHash()
	if debug {
		log.Println("DB: Debug: Records modified... Saving")
	}
}

func (c *Collection) getHash() string {
	h, errH := hashInput(c.Records)
	if errH != nil {
		log.Println("DB: there was a problem hashing records:", errH)
	}
	return h
}

func (c *Collection) saveRecordsToFile() error {
	c.Lock()
	defer c.Unlock()
	jsonBytes, errMarshal := json.Marshal(c.Records)
	if errMarshal != nil {
		return errWrap(errMarshal, "DB: couldn't marshal records")
	}

	compressedJsonBytes, errCompress := compress(jsonBytes)
	if errCompress != nil {
		return errWrap(errMarshal, "DB: couldn't compress records")
	}

	errSaveJsonFile := filekit.WriteToFile(filePath, compressedJsonBytes)
	if errSaveJsonFile != nil {
		return errWrap(errSaveJsonFile, "DB: couldn't write records")
	}
	return nil
}

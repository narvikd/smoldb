package smoldb

import (
	cryptoRand "crypto/rand"
	"math/big"
)

// numBetween gives a random int64 based on min and max -> https://stackoverflow.com/a/26153749
func numBetween(min int64, max int64) (int64, error) {
	newMax := big.NewInt(max - min + 1)
	// get big.Int between 0 and newMax
	randNum, err := cryptoRand.Int(cryptoRand.Reader, newMax)
	if err != nil {
		return 0, errWrap(err, "error converting int to bigNum on numBetween")
	}
	// add min to randNum to return a number in the range of the input
	return randNum.Int64() + min, nil
}

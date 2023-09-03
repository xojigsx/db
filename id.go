package qp

import (
	"math/rand"
	"strings"
)

func ID(prefix string) string {
	if len(prefix) > 7 {
		panic("prefix must be less than 8 characters: " + prefix)
	}
	return prefix + "-" + newID(16)
}

const (
	chars = "abcdefghjklmnopqrstvwxyzABCDEFGHJKLMNOPQRSTVWXYZ023456789"
)

func newID(n int) string {
	return randString(chars, 6, 1<<6-1, 63/6, n)
}

// borrowed from https://stackoverflow.com/questions/22892120/
func randString(chars string, idxBits uint, idxMask int64, idxMax int, n int) string {
	var sb strings.Builder

	sb.Grow(n)

	for i, cache, remain := n-1, rand.Int63(), idxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), idxMax
		}
		if idx := int(cache & idxMask); idx < len(chars) {
			sb.WriteByte(chars[idx])
			i--
		}
		cache >>= idxBits
		remain--
	}

	return sb.String()
}

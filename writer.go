package main

import (
	"encoding/json"
	"io"
	"time"
)

type AuditWriter interface {
	Write(*AuditMessageGroup) error
}

type DefaultAuditWriter struct {
	e        *json.Encoder
	w        io.Writer
	attempts int
}

func NewDefaultAuditWriter(w io.Writer, attempts int) *DefaultAuditWriter {
	return &DefaultAuditWriter{
		e:        json.NewEncoder(w),
		w:        w,
		attempts: attempts,
	}
}

func (a *DefaultAuditWriter) Write(msg *AuditMessageGroup) (err error) {
	for i := 0; i < a.attempts; i++ {
		err = a.e.Encode(msg)
		if err == nil {
			break
		}

		if i != a.attempts {
			// We have to reset the encoder because write errors are kept internally and can not be retried
			a.e = json.NewEncoder(a.w)
			el.Println("Failed to write message, retrying in 1 second. Error:", err)
			time.Sleep(time.Second * 1)
		}
	}

	return err
}

package main

import (
	"encoding/json"
	"io"
	"os"
	"time"

	"gopkg.in/Graylog2/go-gelf.v2/gelf"
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

func NewGELFAuditWriter(writer gelf.Writer, attempts int, extra map[string]interface{}) *GELFAuditWriter {

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "UNNAMED_HOST"
	}

	return &GELFAuditWriter{
		writer:   writer,
		hostname: hostname,
		attempts: attempts,
		extra:    extra,
	}
}

type GELFAuditWriter struct {
	attempts int
	hostname string
	writer   gelf.Writer
	extra    map[string]interface{}
}

func (gw *GELFAuditWriter) Write(msg *AuditMessageGroup) error {

	rawData, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	for i := 0; i < gw.attempts; i++ {
		err = gw.writer.WriteMessage(&gelf.Message{
			Version:  "1.1",
			Host:     gw.hostname,
			Short:    string(rawData),
			TimeUnix: float64(time.Now().Unix()),
			Extra:    gw.extra,
		})

		if err == nil {
			break
		}

		if i != gw.attempts {
			el.Println("Failed to write message, retrying in 1 second. Error:", err)
			time.Sleep(time.Second)
		}
	}

	return err
}

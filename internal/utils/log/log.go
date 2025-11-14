// MIT License
//
// Copyright (c) 2025 Aleksandr A. Lomov
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software
// and associated documentation files (the “Software”), to deal in the Software without
// restriction, including without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or
// substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.

package ulog

import (
	"context"
	"net/http/httptrace"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func InitConsoleWriter(l zerolog.Level) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
	log.Logger = log.Logger.Level(l).With().Caller().Logger()
}

func ContextWithHTTPTracer(ctx context.Context, name string) context.Context {
	return httptrace.WithClientTrace(ctx, HTTPTracer(name))
}

func HTTPTracer(name string) *httptrace.ClientTrace {
	const reqTypeLogKey = "req-type"
	var startDNS, startConn, startReq, startResp time.Time

	return &httptrace.ClientTrace{
		GetConn: func(hostPort string) {
			log.Trace().Str(reqTypeLogKey, name).Msgf("connecting to %s", hostPort)
			startConn = time.Now()
		},
		GotConn: func(info httptrace.GotConnInfo) {
			log.Trace().Str(reqTypeLogKey, name).Msgf(
				"connection reused: %v, idle: %v, wasIdle: %v, idleTime: %v",
				info.Reused,
				info.Reused,
				info.WasIdle,
				info.IdleTime,
			)
			log.Trace().Str(reqTypeLogKey, name).Msgf("gotConn after %v", time.Since(startConn))
		},
		DNSStart: func(_ httptrace.DNSStartInfo) {
			startDNS = time.Now()
		},
		DNSDone: func(_ httptrace.DNSDoneInfo) {
			log.Trace().Str(reqTypeLogKey, name).Msgf("DNS resolved in %v", time.Since(startDNS))
		},
		WroteRequest: func(_ httptrace.WroteRequestInfo) {
			startReq = time.Now()
			log.Trace().Str(reqTypeLogKey, name).Msgf("request sent")
		},
		GotFirstResponseByte: func() {
			startResp = time.Now()
			log.Trace().Str(reqTypeLogKey, name).Msgf("got first byte after %v", startResp.Sub(startReq))
		},
		PutIdleConn: func(err error) {
			log.Printf("putIdle err=%v", err)
		},
	}
}

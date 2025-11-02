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

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"s3testcase/internal/storage/filestorage"
	"s3testcase/internal/storagelocator"
	"s3testcase/internal/storageservice"
	ulog "s3testcase/internal/utils/log"
)

func main() {
	ulog.InitConsoleWriter(zerolog.TraceLevel)

	fs := filestorage.NewStorage(filestorage.LoadConfig())
	if err := fs.Run(); err != nil {
		log.Fatal().Err(err).Msg("failed to run storage")
		return
	}

	ls := storagelocator.NewLiveSender(storagelocator.LoadLiveSenderConfig(), fs)
	service := storageservice.NewServer(storageservice.LoadConfig(), ls, fs)

	srvCh := service.Run()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer close(signals)
	defer signal.Stop(signals)

	select {
	case <-signals:
		log.Debug().Msg("signal received - stopping service")
	case err := <-srvCh:
		if err != nil {
			log.Error().Err(err).Msg("service crashed")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := service.Shutdown(ctx); err != nil {
		log.Err(err).Msgf("service shutdown failed")
	}
	if err := fs.Shutdown(ctx); err != nil {
		log.Err(err).Msgf("storage shutdown failed")
	}
}

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

package db

import (
	"fmt"
	"net/url"
	"strconv"

	usys "s3testcase/internal/utils/sys"
)

type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

func LoadConfig() Config {
	port, _ := strconv.Atoi(usys.GetEnv("DB_PORT", "5432"))

	return Config{
		Host:     usys.GetEnv("DB_HOST", "localhost"),
		Port:     port,
		User:     usys.GetEnv("DB_USER", "postgres"),
		Password: usys.GetEnv("DB_PASSWORD", "password"),
		DBName:   usys.GetEnv("DB_NAME", "postgres"),
		SSLMode:  usys.GetEnv("DB_SSLMODE", "disable"),
	}
}

func (c Config) DSN() string {
	password := url.QueryEscape(c.Password)
	return fmt.Sprintf(
		"postgresql://%s:%s@%s:%d/%s?sslmode=%s",
		c.User, password, c.Host, c.Port, c.DBName, c.SSLMode,
	)
}

// SafeDSN returns DSN with hidden password for logs.
func (c Config) SafeDSN() string {
	return fmt.Sprintf(
		"postgres://%s:*****@%s:%d/%s?sslmode=%s",
		c.User, c.Host, c.Port, c.DBName, c.SSLMode,
	)
}

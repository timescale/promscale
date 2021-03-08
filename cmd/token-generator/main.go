package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/timescale/promscale/pkg/jwt/generator"
)

func main() {
	config := new(generator.Config)
	var err error
	if err = generatorFlags(config, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "msg", "failed to parse flags", "error", err)
		os.Exit(1)
	}

	if err = config.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, "msg", "failed to validate generator config", "error", err)
		os.Exit(1)
	}

	if err = config.FillKeyFromFile(); err != nil {
		fmt.Fprintln(os.Stderr, "msg", "failed to fill key from file", "error", err)
		os.Exit(1)
	}

	token, err := generator.GenerateToken(config)
	if err != nil {
		fmt.Fprintln(os.Stderr, "msg", "failed to generate token", "error", err)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stdout, "token", token)
}

func generatorFlags(config *generator.Config, args []string) error {
	flag.StringVar(&config.PrivateKey, "private-key", "", "Private key value based on RSA algorithm. "+
		"It is mutually exclusive with 'private-key-file'. Supported bits: [512, 1024, 2048, 4096]")
	flag.StringVar(&config.PrivateKeyPath, "private-key-path", "", "Path of the file containing private "+
		"key value, based on RSA algorithm. It is mutually exclusive with 'private-key-file'. "+
		"Supported bits: [512, 1024, 2048, 4096]")
	flag.StringVar(&config.Issuer, "jwt-issuer", "", "Principal that issued the JWT.")
	// TODO(harkishen): Make audience as list of audiences.
	flag.StringVar(&config.Audience, "jwt-audience", "", "Recipients that the JWT is intended for.")
	var issuedAt, expireAt string
	flag.StringVar(&issuedAt, "jwt-issued-at", "", "Time at which the JWT was issued. If you leave this blank, "+
		"it will automatically set to now.")
	flag.StringVar(&expireAt, "jwt-expire-at", "", "Time at which the JWT will expire. If you leave this blank, "+
		"then the token would never expire.")
	_ = flag.CommandLine.Parse(args)

	var (
		t     time.Time
		apply bool
		err   error
	)
	t, apply, err = convertStrTimeToTime(issuedAt)
	if err != nil {
		return fmt.Errorf("generator flags: %w", err)
	}
	if apply {
		config.IssuedAt = t
	}
	t, apply, err = convertStrTimeToTime(expireAt)
	if err != nil {
		return fmt.Errorf("generator flags: %w", err)
	}
	if apply {
		config.ExpireAt = t
	}
	return nil
}

func convertStrTimeToTime(st string) (time.Time, bool, error) {
	if st == "" {
		return time.Time{}, false, nil
	}
	t, err := time.Parse(st, http.TimeFormat)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("convert string time to time: %w", err)
	}
	return t, true, nil
}

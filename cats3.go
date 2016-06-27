package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/dustin/go-humanize"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
)

const VERSION = "0.0.1"

var errorLog *log.Logger = log.New(os.Stderr, "ERROR: ", log.LstdFlags)
var infoLog *log.Logger = log.New(os.Stderr, "INFO: ", log.LstdFlags)

func listAllObjects(output chan string, svc *s3.S3, bucket string, delimiter string, prefix string) {
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	if delimiter != "" {
		params.Delimiter = aws.String(delimiter)
	}

	for {
		resp, err := svc.ListObjectsV2(params)

		if err != nil {
			errorLog.Println(err)
			// TODO: stop command with exit(1)
			return
		}

		for _, obj := range resp.Contents {
			output <- *obj.Key

			infoLog.Println("object", *obj.Key, humanize.Bytes(uint64(*obj.Size)))
		}

		for _, commonPrefix := range resp.CommonPrefixes {
			infoLog.Println("prefix", *commonPrefix.Prefix)
		}

		if *resp.IsTruncated {
			params.ContinuationToken = resp.NextContinuationToken
		} else {
			break
		}
	}
}

func cat(svc *s3.S3, bucket string, key string) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	resp, err := svc.GetObject(params)
	if err != nil {
		// XXX: may I just log this and continue next key?
		errorLog.Fatal(err)
	}

	if _, err := io.Copy(os.Stdout, resp.Body); err != nil {
		errorLog.Fatal(err)
	}
}

func main() {
	cmdName := path.Base(os.Args[0])

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] key [key...]\n", cmdName)
		fmt.Fprintf(os.Stderr, "options:\n")
		flag.PrintDefaults()
	}

	var bucket string
	var argsArePrefix bool
	var delimiter string
	var dryRun bool
	var quiet bool
	var showVersion bool

	flag.StringVar(&bucket, "bucket", "", "bucket name (*required*)")
	flag.BoolVar(&argsArePrefix, "prefix", false, "treat args as a prefix (get all objects matching it)")
	flag.StringVar(&delimiter, "delimiter", "/", "delemiter to get list")
	flag.BoolVar(&dryRun, "dry-run", false, "don't get object but print keys only")
	flag.BoolVar(&quiet, "quiet", false, "surpress info message")
	flag.BoolVar(&showVersion, "version", false, "")

	flag.Parse()

	if showVersion {
		fmt.Printf("%s %s\n", cmdName, VERSION)
		os.Exit(0)
	}

	if bucket == "" {
		errorLog.Fatal("bucket name required")
	}

	if quiet {
		infoLog.SetOutput(ioutil.Discard)
	}

	svc := s3.New(session.New())
	keys := make(chan string, 1)

	// get keys in parallel
	go func() {
		defer close(keys)
		for _, arg := range flag.Args() {
			if argsArePrefix {
				listAllObjects(keys, svc, bucket, delimiter, arg)
			} else {
				keys <- arg
				infoLog.Println("object", arg)
			}
		}
	}()

	for key := range keys {
		if dryRun {
			// just consume key, but do nothing
			continue
		} else {
			cat(svc, bucket, key)
		}
	}
}

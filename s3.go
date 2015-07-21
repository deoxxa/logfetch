package main

import (
	"bufio"
	"compress/gzip"
	"crypto"
	"encoding/hex"
	"io"
	"os"
	"os/user"
	"path"
	"regexp"
	"strings"
	"time"

	"fknsrs.biz/p/seendb"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	awsAccessKey = app.Flag("aws_access_key", "AWS access key.").OverrideDefaultFromEnvar("AWS_ACCESS_KEY").Required().String()
	awsSecretKey = app.Flag("aws_secret_key", "AWS secret key.").OverrideDefaultFromEnvar("AWS_SECRET_KEY").Required().String()
	awsRegion    = app.Flag("aws_region", "AWS region.").OverrideDefaultFromEnvar("AWS_DEFAULT_REGION").String()
)

type s3Options struct {
	bucket  string
	prefix  string
	include string
	exclude string
}

func readFromS3(options s3Options) <-chan map[string]interface{} {
	ch := make(chan map[string]interface{})

	c := s3.New(&aws.Config{
		Credentials: credentials.NewStaticCredentials(*awsAccessKey, *awsSecretKey, ""),
		Region:      *awsRegion,
	})

	u, err := user.Current()
	if err != nil {
		panic(err)
	}

	var includeRegexp *regexp.Regexp
	if options.include != "" {
		includeRegexp = regexp.MustCompile(options.include)
	}

	var excludeRegexp *regexp.Regexp
	if options.exclude != "" {
		excludeRegexp = regexp.MustCompile(options.exclude)
	}

	sha := crypto.SHA1.New()
	sha.Write([]byte(strings.Join([]string{
		options.bucket,
		options.prefix,
		options.include,
		options.exclude,
	}, "$$")))
	stateFile := path.Join(u.HomeDir, ".logfetch", "s3_"+hex.EncodeToString(sha.Sum(nil)))

	if err := os.MkdirAll(path.Dir(stateFile), 0755); err != nil {
		panic(err)
	}

	seen, err := seendb.New(stateFile)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			marker := ""

		outer:
			for {
				listConfig := s3.ListObjectsInput{
					Bucket: aws.String(options.bucket),
				}

				if options.prefix != "" {
					listConfig.Prefix = aws.String(options.prefix)
				}

				if marker != "" {
					listConfig.Marker = aws.String(marker)
				}

				res, err := c.ListObjects(&listConfig)
				if err != nil {
					break outer
				}

			inner:
				for _, o := range res.Contents {
					if includeRegexp != nil {
						if !includeRegexp.MatchString(*o.Key) {
							continue
						}
					}

					if excludeRegexp != nil {
						if excludeRegexp.MatchString(*o.Key) {
							continue
						}
					}

					if seen.Seen(*o.Key) {
						continue
					}

					fileConfig := s3.GetObjectInput{
						Bucket: aws.String(options.bucket),
						Key:    o.Key,
					}

					f, err := c.GetObject(&fileConfig)
					if err != nil {
						continue inner
					}
					defer f.Body.Close()

					b := f.Body

					if strings.HasSuffix(*o.Key, ".gz") {
						gz, err := gzip.NewReader(b)
						if err != nil {
							if err := seen.Mark(*o.Key); err != nil {
								panic(err)
							}

							continue inner
						}
						defer gz.Close()

						b = gz
					}

					r := bufio.NewReader(b)

					for {
						l, err := r.ReadString('\n')
						if err == io.EOF {
							break
						} else if err != nil {
							break
						}

						l = strings.TrimSpace(l)
						if l == "" {
							continue
						}

						ch <- map[string]interface{}{
							"text":      l,
							"s3_bucket": options.bucket,
							"s3_key":    *o.Key,
						}
					}

					if err := seen.Mark(*o.Key); err != nil {
						panic(err)
					}
				}

				if !*res.IsTruncated {
					break
				}

				marker = *res.Contents[len(res.Contents)-1].Key
			}

			time.Sleep(time.Minute)
		}
	}()

	return ch
}

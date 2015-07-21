package logfetch

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
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Options struct {
	AWSConfig *aws.Config
	Bucket    string
	Prefix    string
	Include   string
	Exclude   string
	StateFile string
}

func ReadFromS3(options S3Options) <-chan map[string]interface{} {
	ch := make(chan map[string]interface{})

	cfg := options.AWSConfig
	if cfg == nil {
		cfg = aws.DefaultConfig
	}

	c := s3.New(cfg)

	var includeRegexp *regexp.Regexp
	if options.Include != "" {
		includeRegexp = regexp.MustCompile(options.Include)
	}

	var excludeRegexp *regexp.Regexp
	if options.Exclude != "" {
		excludeRegexp = regexp.MustCompile(options.Exclude)
	}

	stateFile := options.StateFile

	if stateFile == "" {
		u, err := user.Current()
		if err != nil {
			panic(err)
		}

		sha := crypto.SHA1.New()
		sha.Write([]byte(strings.Join([]string{
			options.Bucket,
			options.Prefix,
			options.Include,
			options.Exclude,
		}, "$$")))

		stateFile = path.Join(u.HomeDir, ".logfetch", "s3_"+hex.EncodeToString(sha.Sum(nil)))
	}

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
					Bucket: aws.String(options.Bucket),
				}

				if options.Prefix != "" {
					listConfig.Prefix = aws.String(options.Prefix)
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
						Bucket: aws.String(options.Bucket),
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

					var i int

					for {
						l, err := r.ReadString('\n')
						if err == io.EOF {
							break
						} else if err != nil {
							break
						}

						i++

						l = strings.TrimSpace(l)
						if l == "" {
							continue
						}

						ch <- map[string]interface{}{
							"text":      l,
							"s3_bucket": options.Bucket,
							"s3_key":    *o.Key,
							"s3_line":   i,
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

package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"

	ebsfile "github.com/masahiro331/go-ebs-file"
)

var _ aws.CredentialsProvider = &DummyCredentialProvider{}

type DummyCredentialProvider struct{}

func (p *DummyCredentialProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID:     "YOUR_ACCESS_KEY",
		SecretAccessKey: "YOUR_SECRET_KEY",
	}, nil
}

func main() {
	var rs io.ReadSeeker
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("YOUR_REGION"),
		config.WithCredentialsProvider(&DummyCredentialProvider{}),
	)
	if err != nil {
		log.Fatal(err)
	}

	cli, err := ebsfile.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	rs, err = ebsfile.Open(os.Args[1], context.Background(), nil, cli)
	if err != nil {
		log.Fatal(err)
	}

	buf := make([]byte, 512)
	rs.Read(buf)
	fmt.Println(hex.Dump(buf))
}

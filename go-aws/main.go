package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/joho/godotenv"
	"github.com/mogalmahesh/binary-guy-blog-code/go-aws/s3"
	"log"
	"os"
)

func main() {
	log.Println("Starting...")

	err := godotenv.Load("../.env")

	if err != nil {
		log.Fatal("Error loading .env file. \n", err)
	}

	awsProfile := os.Getenv("AWS_PROFILE")
	log.Printf("Using profile named %s to work with AWS", awsProfile)

	conf, err := getAWSDefaultConfig(awsProfile)

	if err != nil {
		log.Fatal("Unable to read default config \n", err)
	}

	awsS3(conf)
	log.Println("Done...")
}

func getAWSDefaultConfig(profileName string) (aws.Config, error) {
	// using aws cli profile
	// replace profile name with one you have configured on your system
	return config.LoadDefaultConfig(context.TODO(), config.WithSharedConfigProfile(profileName))
}

func awsS3(conf aws.Config) {
	// s3.ListBuckets(conf)
	// s3.CreateBucket(conf, "bucket-created-using-go-2")
	// s3.DeleteBucket(conf, "bucket-created-using-go")
	// s3.ListS3Objects(conf, "my-test-bucket-123df")
	// s3.ListS3ObjectsWithPaginator(conf, "my-test-bucket-123df")
	// s3.PutFileToS3(conf, "my-test-bucket-123df", "main.go", "./")
	// s3.PutFileToS3WithMultiPartUpload(conf, "my-test-bucket-123df", "main.go", "./")
	// s3.CopyS3Object(conf, "my-test-bucket-123df", "my-test-bucket-123df/2023-01-07/dynamoDB-aws.jpg", "copy/dynamoDB-aws.jpg")
	// s3.DownloadS3Object(conf, "my-test-bucket-123df", "2023-01-08-10/09719495-15b1-41c9-bcff-ccc0d62d1f30/dynamoDB-aws.png")
	// s3.DeleteS3Object(conf, "my-test-bucket-123df", "main.go")

	// var exitingTagSet = s3.GetS3ObjectTags(conf, "my-test-bucket-123df", "2023-01-07/dynamoDB-aws.jpg")

	// name variable tagSet when using
	var _ = []types.Tag{{Key: aws.String("stage"), Value: aws.String("test")}}

	// exitingTagSet = append(exitingTagSet, tagSet...)
	// s3.TagS3Object(conf, "my-test-bucket-123df", "2023-01-07/dynamoDB-aws.jpg", exitingTagSet)
	s3.GetS3ObjectACL(conf, "my-test-bucket-123df", "2023-01-07/dynamoDB-aws.jpg")
	s3.SetS3ObjectACL(conf, "my-test-bucket-123df", "2023-01-07/dynamoDB-aws.jpg", types.ObjectCannedACLPublicRead)
}

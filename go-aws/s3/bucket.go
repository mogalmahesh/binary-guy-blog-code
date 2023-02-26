package s3

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log"
)

func ListBuckets(conf aws.Config) {

	s3Client := getS3Client(conf)

	result, err := s3Client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	var buckets []types.Bucket
	if err != nil {
		log.Printf("Error listing bcukets: %v\n", err)
	} else {
		buckets = result.Buckets
	}

	for _, bucket := range buckets {
		fmt.Printf("%v\t%v\n", *bucket.CreationDate, *bucket.Name)
	}
}

func CreateBucket(conf aws.Config, bucketName string) {
	s3Client := getS3Client(conf)

	_, err := s3Client.CreateBucket(
		context.TODO(),
		&s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
			// CreateBucketConfiguration: &types.CreateBucketConfiguration{LocationConstraint: types.BucketLocationConstraint("us-east-1")},
		})

	if err != nil {
		log.Println("Error creating bucket ", err)
	} else {
		log.Println("Bucket created")
	}
}

func DeleteBucket(conf aws.Config, bucketName string) {
	s3Client := getS3Client(conf)

	_, err := s3Client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		log.Println("Error deleting bucket ", err)
	} else {
		log.Println("Bucket deleted")
	}
}

func getS3Client(conf aws.Config) *s3.Client {
	return s3.NewFromConfig(conf)
}

package s3

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/gabriel-vasile/mimetype"
	"io"
	"log"
	"os"
	"path/filepath"
)

func ListS3Objects(conf aws.Config, bucketName string) {
	s3Client := getS3Client(conf)

	result, err := s3Client.ListObjectsV2(
		context.TODO(),
		&s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		},
	)

	if err != nil {
		log.Println("Error listing objects in bucket ", err)
	} else {
		var contents []types.Object
		contents = result.Contents
		for _, data := range contents {
			log.Printf("object %v", *data.Key)
		}
	}
}

func ListS3ObjectsWithPaginator(conf aws.Config, bucketName string) {
	s3Client := getS3Client(conf)

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	paginator := s3.NewListObjectsV2Paginator(s3Client, params, func(o *s3.ListObjectsV2PaginatorOptions) { o.Limit = 10 })

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(context.TODO())

		if err != nil {
			log.Printf("Error reading paginated list of objects %v", err)
			return
		}
		for _, data := range output.Contents {
			log.Println(*data.Key)
		}
		log.Println("### Done with page. ###")
	}
}

func PutFileToS3(conf aws.Config, bucket string, fileName string, path string) {
	s3Client := getS3Client(conf)

	file, err := os.Open(filepath.Join(path, fileName))

	if err != nil {
		log.Println("Error reading file ", err)
		return
	}

	mimeType, err := mimetype.DetectReader(file)

	if err != nil {
		log.Println("Error reading file content type ", err)
		return
	}

	log.Println(mimeType.String())
	file1, err := os.Open(filepath.Join(path, fileName))

	_, err1 := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(fileName),
		Body:        file1,
		ContentType: aws.String(mimeType.String()),
	})

	if err1 != nil {
		log.Println("Error uploading file ", err1)
		return
	}

	log.Println("File uploaded")
}

func PutFileToS3WithMultiPartUpload(conf aws.Config, bucket string, fileName string, path string) {
	s3Client := getS3Client(conf)

	file, err := os.Open(filepath.Join(path, fileName))

	if err != nil {
		log.Println("Error reading file ", err)
		return
	}

	mimeType, err := mimetype.DetectReader(file)

	if err != nil {
		log.Println("Error reading file content type ", err)
		return
	}

	uploader := manager.NewUploader(s3Client, func(uploader *manager.Uploader) {
		// This will upload file in 5MB chuck and upload 10 chucks in parallel
		uploader.PartSize = 5 * 1024 * 1024 // 5MB
		uploader.Concurrency = 10
	})

	file1, err := os.Open(filepath.Join(path, fileName))

	_, err1 := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(fileName),
		Body:        file1,
		ContentType: aws.String(mimeType.String()),
	})

	if err1 != nil {
		log.Println("Error uploading file ", err1)
		return
	}

	log.Println("File uploaded")
}

func CopyS3Object(conf aws.Config, bucketName string, source string, destination string) {
	s3Client := getS3Client(conf)

	_, err := s3Client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		CopySource: aws.String(source),
		Key:        aws.String(destination),
	})

	if err != nil {
		log.Println("Error copying file ", err)
		return
	}

	log.Println("Object copied ")
}

func DownloadS3Object(conf aws.Config, bucket string, key string) {
	s3Client := getS3Client(conf)

	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := s3Client.GetObject(context.TODO(), params)

	if err != nil {
		log.Println("Error downloading file ", err)
		return
	}

	outFile, err := os.Create("test.png")

	_, err1 := io.Copy(outFile, result.Body)

	if err1 != nil {
		log.Println("Error uploading file ", err1)
		return
	}

	log.Println("File downloaded")
}

func DeleteS3Object(conf aws.Config, bucket string, key string) {
	s3Client := getS3Client(conf)

	_, err := s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		log.Println("Error deleting file ", err)
		return
	}

	log.Println("File deleted..")
}

func GetS3ObjectTags(conf aws.Config, bucketName string, key string) []types.Tag {
	s3Client := getS3Client(conf)

	result, err := s3Client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})

	if err != nil {
		log.Println("Error getting tags for s3 object ", err)
	}

	return result.TagSet
}

func TagS3Object(conf aws.Config, bucketName string, key string, tagSet []types.Tag) {
	s3Client := getS3Client(conf)

	var tagging = types.Tagging{TagSet: tagSet}

	_, err := s3Client.PutObjectTagging(context.TODO(), &s3.PutObjectTaggingInput{
		Bucket:  aws.String(bucketName),
		Key:     aws.String(key),
		Tagging: &tagging,
	})

	if err != nil {
		log.Println("Error adding tags to object ", err)
		return
	}

	log.Println("TagSet added to object")
}

func GetS3ObjectACL(conf aws.Config, bucketName string, key string) {
	s3Client := getS3Client(conf)

	result, err := s3Client.GetObjectAcl(context.TODO(), &s3.GetObjectAclInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})

	if err != nil {
		log.Println("Error reading object ACL ", err)
		return
	}

	for _, grant := range result.Grants {
		var displayName string
		if grant.Grantee.DisplayName == nil {
			displayName = "None"
		} else {
			displayName = *grant.Grantee.DisplayName
		}
		log.Printf("Grantee: %v, Permission: %v \n", displayName, grant.Permission)
	}
	log.Printf("Owner %v \n", *result.Owner.DisplayName)
}

func SetS3ObjectACL(conf aws.Config, bucketName string, key string, acl types.ObjectCannedACL) {
	s3Client := getS3Client(conf)

	_, err := s3Client.PutObjectAcl(context.TODO(), &s3.PutObjectAclInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		ACL:    types.ObjectCannedACL(acl),
	})

	if err != nil {
		log.Println("Error putting ACL on s3 object ", err)
		return
	}

	log.Println("S3 Object ACL updated.")
}

package s3api

import (
	"encoding/xml"
	"time"
)

const s3NS = "http://s3.amazonaws.com/doc/2006-03-01/"

type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
	Owner   Owner    `xml:"Owner"`
	Buckets Buckets  `xml:"Buckets"`
}

type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type Buckets struct {
	Bucket []BucketEntry `xml:"Bucket"`
}

type BucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

// ListBucketResult serves both ListObjects (v1) and ListObjectsV2; the fields
// that do not apply to a version are left empty and omitted.
type ListBucketResult struct {
	XMLName               xml.Name       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	KeyCount              int            `xml:"KeyCount,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Marker                *string        `xml:"Marker,omitempty"`
	NextMarker            string         `xml:"NextMarker,omitempty"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	Contents              []ObjectEntry  `xml:"Contents"`
	CommonPrefixes        []CommonPrefix `xml:"CommonPrefixes,omitempty"`
}

type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type ObjectEntry struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type LocationConstraint struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint"`
	Region  string   `xml:",chardata"`
}

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type CompleteMultipartUploadRequest struct {
	XMLName xml.Name                `xml:"CompleteMultipartUpload"`
	Parts   []CompleteMultipartPart `xml:"Part"`
}

type CompleteMultipartPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type DeleteObjectsRequest struct {
	XMLName xml.Name              `xml:"Delete"`
	Quiet   bool                  `xml:"Quiet"`
	Objects []DeleteObjectPayload `xml:"Object"`
}

type DeleteObjectPayload struct {
	Key string `xml:"Key"`
}

type DeleteResult struct {
	XMLName xml.Name        `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult"`
	Deleted []DeletedObject `xml:"Deleted"`
	Error   []DeleteError   `xml:"Error,omitempty"`
}

type DeletedObject struct {
	Key string `xml:"Key"`
}

type DeleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type S3ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestId string   `xml:"RequestId"`
}

func FormatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

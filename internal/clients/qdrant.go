package clients

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	pb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type QdrantClient struct {
	conn       *grpc.ClientConn
	points     pb.PointsClient
	collection string
}

func NewQdrantClient(host string, port int, collection string) (*QdrantClient, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("qdrant grpc dial: %w", err)
	}
	return &QdrantClient{
		conn:       conn,
		points:     pb.NewPointsClient(conn),
		collection: collection,
	}, nil
}

func (q *QdrantClient) Close() error {
	return q.conn.Close()
}

type ChunkPayload struct {
	Text       string
	Index      int
	TokenCount int
}

func (q *QdrantClient) UpsertPoints(ctx context.Context, paperID string, chunks []ChunkPayload, embeddings [][]float32) error {
	points := make([]*pb.PointStruct, len(chunks))
	for i, chunk := range chunks {
		pointID := uuid.New().String()
		points[i] = &pb.PointStruct{
			Id: &pb.PointId{
				PointIdOptions: &pb.PointId_Uuid{Uuid: pointID},
			},
			Vectors: &pb.Vectors{
				VectorsOptions: &pb.Vectors_Vector{
					Vector: &pb.Vector{Data: embeddings[i]},
				},
			},
			Payload: map[string]*pb.Value{
				"paper_id":    {Kind: &pb.Value_StringValue{StringValue: paperID}},
				"chunk_index": {Kind: &pb.Value_IntegerValue{IntegerValue: int64(chunk.Index)}},
				"text":        {Kind: &pb.Value_StringValue{StringValue: chunk.Text}},
				"token_count": {Kind: &pb.Value_IntegerValue{IntegerValue: int64(chunk.TokenCount)}},
			},
		}
	}

	wait := true
	_, err := q.points.Upsert(ctx, &pb.UpsertPoints{
		CollectionName: q.collection,
		Wait:           &wait,
		Points:         points,
	})
	if err != nil {
		return fmt.Errorf("qdrant upsert: %w", err)
	}
	return nil
}

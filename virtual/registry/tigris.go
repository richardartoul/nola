package registry

import (
	"context"
	"time"

	"github.com/richardartoul/nola/virtual/types"
	"github.com/tigrisdata/tigris-client-go/tigris"
)

type tigrisRegistry struct {
	db *tigris.Database
}

// TigrisModule is the model for the Module's collection in Tigris.
type TigrisModule struct {
	tigris.Model
	ID      string
	Data    []byte
	Options ModuleOptions
}

func NewTigris(
	url string,
) (Registry, error) {
	ctx, cc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cc()

	// // Connect to the Tigris server
	// client, err := tigris.NewClient(ctx, &tigris.Config{URL: "localhost:8081", Project: "nola"})
	// if err != nil {
	// 	panic(err)
	// }
	// defer client.Close()

	// // // Create the database and collection if they don't exist,
	// // // otherwise update the schema of the collection if it already exists
	// // db, err := client.OpenDatabase(ctx, "hello_db", &TigrisModule{})
	// // if err != nil {
	// // 	panic(err)
	// // }

	// // cfg := &tigris.Config{
	// // 	// Driver: config.Driver{
	// // 	URL: "localhost:8081",
	// // 	// Project: "nola",
	// // 	// 	// ClientID:     "your_client_id_here",
	// // 	// 	// ClientSecret: "your_client_secret_here",
	// // 	// },
	// // }

	// db, err := client.OpenDatabase(ctx, &TigrisModule{})
	// if err != nil {
	// 	return nil, fmt.Errorf("error opening tigris database: %w", err)
	// }

	// if err := db.CreateCollections(ctx, &TigrisModule{}); err != nil {
	// 	return nil, fmt.Errorf("error creating collections: %w", err)
	// }

	db, err := tigris.OpenDatabase(ctx, &tigris.Config{Project: "nola", URL: url}, &TigrisModule{})
	if err != nil {
		panic(err)
	}

	return &tigrisRegistry{
		db: db,
	}, nil
}

func (t *tigrisRegistry) RegisterModule(
	ctx context.Context,
	namespace,
	moduleID string,
	moduleBytes []byte,
	opts ModuleOptions,
) (RegisterModuleResult, error) {

	panic("not implemented")
}

// GetModule gets the bytes and options associated with the provided module.
func (t *tigrisRegistry) GetModule(
	ctx context.Context,
	namespace,
	moduleID string,
) ([]byte, ModuleOptions, error) {
	panic("not implemented")
}

func (t *tigrisRegistry) CreateActor(
	ctx context.Context,
	namespace,
	actorID,
	moduleID string,
	opts ActorOptions,
) (CreateActorResult, error) {
	panic("not implemented")
}

func (t *tigrisRegistry) IncGeneration(
	ctx context.Context,
	namespace,
	actorID string,
) error {
	panic("not implemented")
}

func (t *tigrisRegistry) EnsureActivation(
	ctx context.Context,
	namespace,
	actorID string,
) ([]types.ActorReference, error) {
	panic("not implemented")
}

func (t *tigrisRegistry) ActorKVPut(
	ctx context.Context,
	namespace string,
	actorID string,
	key []byte,
	value []byte,
) error {
	panic("not implemented")
}

func (t *tigrisRegistry) ActorKVGet(
	ctx context.Context,
	namespace string,
	actorID string,
	key []byte,
) ([]byte, bool, error) {
	panic("not implemented")
}

func (t *tigrisRegistry) Heartbeat(
	ctx context.Context,
	serverID string,
	heartbeatState HeartbeatState,
) error {
	panic("not implemented")
}

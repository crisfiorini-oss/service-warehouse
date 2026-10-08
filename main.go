// Command warehouse is the codefly service agent that runs the warehouse
// gateway as a real service: a container speaking the codefly/warehouse/v0 gRPC
// contract, backed by the embedded engine locally and a cloud warehouse when
// deployed. Consumers depend on it as a gRPC service and never link a warehouse
// SDK — the gateway absorbs every backend difference.
//
// The agent mirrors the shape of the other codefly infrastructure agents
// (codefly-dev/service-object-storage, service-postgres): a Service advertises
// its configuration surface, a Runtime brings the container up for `codefly run`
// and tests, and a Builder emits the Kubernetes deployment and serves the
// image's SBOM evidence. The gateway itself is cmd/service-warehouse; this
// package is only what lets a workspace compose it.
package main

import (
	"context"
	"embed"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/codefly-dev/core/agents"
	"github.com/codefly-dev/core/agents/services"
	"github.com/codefly-dev/core/builders"
	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	agentv0 "github.com/codefly-dev/core/generated/go/codefly/services/agent/v0"
	"github.com/codefly-dev/core/resources"
	runnersbase "github.com/codefly-dev/core/runners/base"
	"github.com/codefly-dev/core/shared"
	"github.com/codefly-dev/core/standards"
	"github.com/codefly-dev/core/templates"
)

// agent is loaded from the embedded agent.codefly.yaml (name + version).
var agent = shared.Must(resources.LoadFromFs[resources.Agent](shared.Embed(infoFS)))

var requirements = builders.NewDependencies(agent.Name,
	builders.NewDependency("service.codefly.yaml"),
)

// configurationGroup is the name of the configuration group this service
// exports to its consumers and reads its own deployment values from.
const configurationGroup = "warehouse"

// Settings are the service.codefly.yaml knobs. They are the local defaults;
// runtime configuration (an environment's values) takes precedence. Credentials
// are never settings: they arrive as runtime configuration, never here.
type Settings struct {
	// Backend selects the warehouse. Create defaults new services to mem, the
	// catalog-only backend. An explicitly empty setting uses the gateway's
	// default, the embedded DuckDB engine.
	Backend string `yaml:"backend"`
	// Database is the database the gateway is bound to (a BigQuery project, a
	// Snowflake or Redshift database, an embedded engine's file).
	Database string `yaml:"database"`
	// Dataset is the default namespace for unqualified names.
	Dataset string `yaml:"dataset"`
	// WarehouseLocation is the region datasets and jobs live in, for the
	// warehouses that have one. It is not named Location because services.Base
	// already has a Location, the service's directory.
	WarehouseLocation string `yaml:"location"`
}

// resolved holds the effective configuration after defaults and runtime
// configuration are applied. The keys are the gateway's own SWH_* variables.
type resolved struct {
	backend  string
	database string
	dataset  string
	location string
	// credentialsFile names a warehouse credentials file. Nothing delivers one:
	// a local run would have to project it into the gateway container, and a
	// deployment mounts nothing, so the Runtime and the Builder both refuse a
	// configured path rather than name a file the gateway cannot open.
	credentialsFile string
	authToken       string
}

type Service struct {
	*services.Base
	*Settings

	conf resolved

	// gatewayToken is the secret every caller must present to the gateway. The
	// Runtime generates one per run and hands it to consumers through the
	// configuration channel as a secret value — never inside the connection
	// string. It lives outside resolved because LoadConfiguration rebuilds
	// resolved from the incoming configuration on every call.
	gatewayToken string

	GrpcEndpoint *basev0.Endpoint
}

func NewService() *Service {
	return &Service{
		Base:     services.NewServiceBase(context.Background(), agent.Of(resources.ServiceAgent)),
		Settings: &Settings{},
	}
}

func (s *Service) GetAgentInformation(ctx context.Context, _ *agentv0.AgentInformationRequest) (*agentv0.AgentInformation, error) {
	readme, err := templates.ApplyTemplateFrom(ctx, shared.Embed(readmeFS), "templates/agent/README.md", s.Information)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	// Validation is left unset on purpose: this agent serves image-scope SBOM
	// (Builder.SBOM) without advertising ValidationCapabilities.image_sbom. A
	// present Validation is authoritative for every operation, including the ones
	// it omits, and advertising the capability is a separate decision from
	// serving the scope: core's docs/sbom.md holds it back until every consumer
	// that evaluates it runs a core whose phase bound accepts it.
	// TestImageSBOMIsServedButNotAdvertised pins both halves.
	return services.Advertisement{
		Backends: runnersbase.BackendSupport{Docker: true},
		ReadMe:   readme,
		Config: []*agentv0.ConfigurationValueDetail{
			{
				Name: configurationGroup, Description: "warehouse gateway endpoint",
				Fields: []*agentv0.ConfigurationValueInformation{
					{Name: "connection", Description: "grpc connection string"},
					{Name: "endpoint", Description: "host:port of the gRPC endpoint"},
					{Name: "token", Description: "shared secret sent as the x-codefly-token grpc metadata header"},
				},
			},
		},
	}.Build(), nil
}

// resolveServingGRPCEndpoint selects the single gRPC endpoint the agent binds
// its runtime and deployment to.
func resolveServingGRPCEndpoint(ctx context.Context, endpoints []*basev0.Endpoint) (*basev0.Endpoint, error) {
	grpc := resources.FindEndpointsByAPI(ctx, standards.GRPC, endpoints)
	if len(grpc) == 0 {
		return nil, fmt.Errorf("no grpc endpoint found")
	}
	return grpc[0], nil
}

// LoadConfiguration resolves the effective configuration. Settings are the local
// defaults; runtime configuration (deployed values) takes precedence.
func (s *Service) LoadConfiguration(ctx context.Context, conf *basev0.Configuration) error {
	r := resolved{
		backend:  s.Backend,
		database: s.Database,
		dataset:  s.Dataset,
		location: s.WarehouseLocation,
	}
	if conf != nil {
		for key, dst := range map[string]*string{
			"SWH_BACKEND":          &r.backend,
			"SWH_DATABASE":         &r.database,
			"SWH_DATASET":          &r.dataset,
			"SWH_LOCATION":         &r.location,
			"SWH_CREDENTIALS_FILE": &r.credentialsFile,
			"SWH_AUTH_TOKEN":       &r.authToken,
		} {
			v, err := resources.GetConfigurationValue(ctx, conf, configurationGroup, key)
			if err == nil && v != "" {
				*dst = v
			}
		}
	}
	// Normalized at the single boundary where it enters, for the same reason the
	// gateway trims its own env read: a secret carrying a trailing newline would
	// otherwise be handed to consumers in one form and enforced in another. The
	// rest arrive through YAML block scalars and env files just the same, and a
	// stray newline in one is not a value but a malformed manifest.
	for _, field := range []*string{&r.backend, &r.database, &r.dataset, &r.location, &r.credentialsFile, &r.authToken} {
		*field = strings.TrimSpace(*field)
	}
	s.conf = r
	return nil
}

func (s *Service) createConnectionString(address string) string {
	return fmt.Sprintf("grpc://%s", address)
}

func (s *Service) CreateConnectionConfiguration(ctx context.Context, conf *basev0.Configuration, instance *basev0.NetworkInstance) (*basev0.Configuration, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)

	if err := s.LoadConfiguration(ctx, conf); err != nil {
		return nil, s.Wool.Wrapf(err, "cannot load configuration")
	}
	values := []*basev0.ConfigurationValue{
		{Key: "connection", Value: s.createConnectionString(instance.Address)},
		{Key: "endpoint", Value: instance.Address},
	}
	// The token travels as its own secret value: putting it in the connection
	// string would leak it into every log line and manifest that carries an
	// endpoint.
	if s.gatewayToken != "" {
		values = append(values, &basev0.ConfigurationValue{Key: "token", Value: s.gatewayToken, Secret: true})
	}
	return &basev0.Configuration{
		Origin:         s.Base.Unique(),
		RuntimeContext: resources.RuntimeContextFromInstance(instance),
		Infos: []*basev0.ConfigurationInformation{
			{Name: configurationGroup, ConfigurationValues: values},
		},
	}, nil
}

func main() {
	svc := NewService()
	agents.Serve(agents.PluginRegistration{
		Agent:   svc,
		Runtime: NewRuntime(),
		Builder: NewBuilder(),
	})
}

//go:embed agent.codefly.yaml
var infoFS embed.FS

//go:embed gateway-image.json
var gatewayImageLockJSON []byte

//go:embed templates/agent
var readmeFS embed.FS

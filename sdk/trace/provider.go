// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package trace // import "go.opentelemetry.io/otel/sdk/trace"

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	export "go.opentelemetry.io/otel/sdk/export/trace"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
)

const (
	defaultTracerName = "go.opentelemetry.io/otel/sdk/tracer"
)

// TODO (MrAlias): unify this API option design:
// https://github.com/open-telemetry/opentelemetry-go/issues/536

// TracerProviderConfig
type TracerProviderConfig struct {
	processors []SpanProcessor

	// defaultSampler is the default sampler used when creating new spans.
	defaultSampler Sampler

	// idGenerator is for internal use only.
	idGenerator IDGenerator

	// spanLimits used to limit the number of attributes, events and links to a span.
	spanLimits SpanLimits

	// resource contains attributes representing an entity that produces telemetry.
	resource *resource.Resource
}

type TracerProviderOption func(*TracerProviderConfig)

type TracerProvider struct {
	mu             sync.Mutex
	namedTracer    map[instrumentation.Library]*tracer
	spanProcessors atomic.Value
	defaultSampler Sampler
	idGenerator    IDGenerator
	spanLimits     SpanLimits
	resource       *resource.Resource
}

var _ trace.TracerProvider = &TracerProvider{}

// NewTracerProvider creates an instance of trace provider. Optional
// parameter configures the provider with common options applicable
// to all tracer instances that will be created by this provider.
func NewTracerProvider(opts ...TracerProviderOption) *TracerProvider {
	o := &TracerProviderConfig{}

	for _, opt := range opts {
		opt(o)
	}

	ensureValidTracerProviderConfig(o)

	tp := &TracerProvider{
		namedTracer:    make(map[instrumentation.Library]*tracer),
		defaultSampler: o.defaultSampler,
		idGenerator:    o.idGenerator,
		spanLimits:     o.spanLimits,
		resource:       o.resource,
	}

	for _, sp := range o.processors {
		tp.RegisterSpanProcessor(sp)
	}

	return tp
}

// Tracer returns a Tracer with the given name and options. If a Tracer for
// the given name and options does not exist it is created, otherwise the
// existing Tracer is returned.
//
// If name is empty, DefaultTracerName is used instead.
//
// This method is safe to be called concurrently.
func (p *TracerProvider) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	c := trace.NewTracerConfig(opts...)

	p.mu.Lock()
	defer p.mu.Unlock()
	if name == "" {
		name = defaultTracerName
	}
	il := instrumentation.Library{
		Name:    name,
		Version: c.InstrumentationVersion,
	}
	t, ok := p.namedTracer[il]
	if !ok {
		t = &tracer{
			provider:               p,
			instrumentationLibrary: il,
		}
		p.namedTracer[il] = t
	}
	return t
}

// RegisterSpanProcessor adds the given SpanProcessor to the list of SpanProcessors
func (p *TracerProvider) RegisterSpanProcessor(s SpanProcessor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	new := spanProcessorStates{}
	if old, ok := p.spanProcessors.Load().(spanProcessorStates); ok {
		new = append(new, old...)
	}
	newSpanSync := &spanProcessorState{
		sp:    s,
		state: &sync.Once{},
	}
	new = append(new, newSpanSync)
	p.spanProcessors.Store(new)
}

// UnregisterSpanProcessor removes the given SpanProcessor from the list of SpanProcessors
func (p *TracerProvider) UnregisterSpanProcessor(s SpanProcessor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	spss := spanProcessorStates{}
	old, ok := p.spanProcessors.Load().(spanProcessorStates)
	if !ok || len(old) == 0 {
		return
	}
	spss = append(spss, old...)

	// stop the span processor if it is started and remove it from the list
	var stopOnce *spanProcessorState
	var idx int
	for i, sps := range spss {
		if sps.sp == s {
			stopOnce = sps
			idx = i
		}
	}
	if stopOnce != nil {
		stopOnce.state.Do(func() {
			if err := s.Shutdown(context.Background()); err != nil {
				otel.Handle(err)
			}
		})
	}
	if len(spss) > 1 {
		copy(spss[idx:], spss[idx+1:])
	}
	spss[len(spss)-1] = nil
	spss = spss[:len(spss)-1]

	p.spanProcessors.Store(spss)
}

// ForceFlush immediately exports all spans that have not yet been exported for
// all the registered span processors.
func (p *TracerProvider) ForceFlush(ctx context.Context) error {
	spss, ok := p.spanProcessors.Load().(spanProcessorStates)
	if !ok {
		return fmt.Errorf("failed to load span processors")
	}
	if len(spss) == 0 {
		return nil
	}

	for _, sps := range spss {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := sps.sp.ForceFlush(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Shutdown shuts down the span processors in the order they were registered.
func (p *TracerProvider) Shutdown(ctx context.Context) error {
	spss, ok := p.spanProcessors.Load().(spanProcessorStates)
	if !ok {
		return fmt.Errorf("failed to load span processors")
	}
	if len(spss) == 0 {
		return nil
	}

	for _, sps := range spss {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var err error
		sps.state.Do(func() {
			err = sps.sp.Shutdown(ctx)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// WithSyncer registers the exporter with the TracerProvider using a
// SimpleSpanProcessor.
func WithSyncer(e export.SpanExporter) TracerProviderOption {
	return WithSpanProcessor(NewSimpleSpanProcessor(e))
}

// WithBatcher registers the exporter with the TracerProvider using a
// BatchSpanProcessor configured with the passed opts.
func WithBatcher(e export.SpanExporter, opts ...BatchSpanProcessorOption) TracerProviderOption {
	return WithSpanProcessor(NewBatchSpanProcessor(e, opts...))
}

// WithSpanProcessor registers the SpanProcessor with a TracerProvider.
func WithSpanProcessor(sp SpanProcessor) TracerProviderOption {
	return func(opts *TracerProviderConfig) {
		opts.processors = append(opts.processors, sp)
	}
}

// WithResource option attaches a resource to the provider.
// The resource is added to the span when it is started.
func WithResource(r *resource.Resource) TracerProviderOption {
	return func(opts *TracerProviderConfig) {
		if r != nil {
			opts.resource = r
		}
	}
}

// WithIDGenerator option registers an IDGenerator with the TracerProvider.
func WithIDGenerator(g IDGenerator) TracerProviderOption {
	return func(opts *TracerProviderConfig) {
		if g != nil {
			opts.idGenerator = g
		}
	}
}

// WithDefaultSampler option registers a DefaultSampler with the the TracerProvider.
func WithDefaultSampler(s Sampler) TracerProviderOption {
	return func(opts *TracerProviderConfig) {
		if s != nil {
			opts.defaultSampler = s
		}
	}
}

// WithSpanLimits option registers a SpanLimits with the the TracerProvider.
func WithSpanLimits(sl SpanLimits) TracerProviderOption {
	return func(opts *TracerProviderConfig) {
		opts.spanLimits = sl
	}
}

// ensureValidTracerProviderConfig ensures that given TracerProviderConfig is valid.
func ensureValidTracerProviderConfig(cfg *TracerProviderConfig) {
	if cfg.defaultSampler == nil {
		cfg.defaultSampler = ParentBased(AlwaysSample())
	}
	if cfg.idGenerator == nil {
		cfg.idGenerator = defaultIDGenerator()
	}
	if cfg.spanLimits.EventCountLimit <= 0 {
		cfg.spanLimits.EventCountLimit = DefaultEventCountLimit
	}
	if cfg.spanLimits.AttributeCountLimit <= 0 {
		cfg.spanLimits.AttributeCountLimit = DefaultAttributeCountLimit
	}
	if cfg.spanLimits.LinkCountLimit <= 0 {
		cfg.spanLimits.LinkCountLimit = DefaultLinkCountLimit
	}
	if cfg.spanLimits.AttributePerEventCountLimit <= 0 {
		cfg.spanLimits.AttributePerEventCountLimit = DefaultAttributePerEventCountLimit
	}
	if cfg.spanLimits.AttributePerLinkCountLimit <= 0 {
		cfg.spanLimits.AttributePerLinkCountLimit = DefaultAttributePerLinkCountLimit
	}
	if cfg.resource == nil {
		cfg.resource = resource.Default()
	}
}

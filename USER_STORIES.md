# Inferflux — User stories

**Product:** [PRODUCT.md](./PRODUCT.md)


### Stream / platform engineer

- As a stream engineer, I want to register a feature transform on a topic with versioned SQL/logic, so that training and inference share the same prep.
- As a stream engineer, I want to choose embed vs RPC per use case with a forced tradeoff checklist, so that teams do not accidentally couple SLAs.
- As a stream engineer, I want replication metadata for hybrid clusters, so that training clouds and edge inference stay coherent.

### ML engineer

- As an ML engineer, I want to publish a model artifact for embedded UDF/Streams use, so that inference stays inside the event path.
- As an ML engineer, I want to attach an RPC model endpoint when organizational policy forbids embed, so that I still get governed rollout and rollback.
- As an ML engineer, I want shadow traffic against a challenger model, so that I can compare accuracy before cutover.

### Data scientist / AutoML user

- As a data scientist, I want to import an AutoML-exported model into the streaming runtime, so that I am not stuck calling a vendor REST API from every event.
- As a data scientist, I want feature definitions reused between historical training extracts and live streams, so that train/serve skew is minimized.

### SRE / security

- As an SRE, I want p99 latency and error budgets on prediction paths, so that ML does not silently degrade the nervous system.
- As a security officer, I want PII inference forced to approved localities, so that raw personal data is not scored in the wrong cloud.
- As a compliance officer, I want provenance of which model version scored which event window, so that decisions are reconstructible.
- As a product owner, I want anomaly or prediction outputs actionable on a downstream topic, so that maintenance or fraud systems can react while the event is still relevant.

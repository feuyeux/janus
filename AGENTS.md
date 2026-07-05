# Repository Guidelines

## Project Structure & Module Organization
This is a Java 25 Maven service for the Janus server. Source code lives under `src/main/java/org/janus/`, organized by concern: `codec/` for binary encoding, `config/`, `discovery/` for Nacos/etcd integration, `grpc/`, `handler/`, `model/`, `observability/`, and `ws/`. Protocol definitions are in `src/main/proto/janus.proto`; generated protobuf/gRPC sources are produced under `target/generated-sources/` and should not be edited. Runtime configuration such as Log4j is in `src/main/resources/`. Architecture, protocol, and usage notes are in `doc/`. Container and observability assets are in `docker/`.

## Build, Test, and Development Commands
- `mvn clean package` — compiles Java, generates protobuf/gRPC classes, runs tests, and builds the shaded jar at `target/janus.jar`.
- `mvn test` — runs the Maven test lifecycle. Add tests under `src/test/java` when introducing behavior.
- `mvn -DskipTests package` — builds quickly when tests are not needed locally.
- `java -jar target/janus.jar` — runs the server using environment variables described in `doc/guide.md` and `JanusServer` comments.
- `docker compose -f docker/docker-compose.yml --project-directory . up --build` — starts the multi-service local stack with Janus instances, Nacos, etcd, Jaeger, Prometheus, Loki, and Grafana. The `--project-directory .` flag is required so that the build context resolves to the project root rather than the `docker/` directory.

## Coding Style & Naming Conventions
Use Java 25 and four-space indentation. Keep package names lowercase under `org.janus`. Name classes in `PascalCase`, methods and fields in `camelCase`, and constants in `UPPER_SNAKE_CASE`. Prefer small, responsibility-focused classes matching the existing package layout. Use SLF4J logging and existing helper classes rather than ad hoc `System.out` output. When changing protobuf messages, update `src/main/proto/janus.proto` first and regenerate via Maven.

## Testing Guidelines
No dedicated test suite is currently present. New functionality should include unit or integration tests under `src/test/java`, mirroring the production package path. Use test names that describe behavior, for example `BinaryCodecTest` or `JanusServiceImplTest`. Run `mvn test` before opening a pull request; use the Docker stack for end-to-end WebSocket, gRPC, discovery, and observability validation.

## Commit & Pull Request Guidelines
Git history follows Conventional Commits, for example `feat: ...` and `refactor(core): ...`; keep messages concise and scoped when useful. Pull requests should include a short summary, validation steps or command output, linked issues when applicable, and screenshots or logs for protocol, Docker, or observability changes.

## Security & Configuration Tips
Do not commit secrets, local credentials, or generated build artifacts. Configure runtime behavior with environment variables and keep local overrides outside version control. Review Docker port mappings before exposing services beyond localhost. When upgrading infrastructure images in `docker/docker-compose.yml`, note that Nacos v3.x requires `NACOS_AUTH_TOKEN` (Base64), `NACOS_AUTH_IDENTITY_KEY`, and `NACOS_AUTH_IDENTITY_VALUE` even in standalone mode; Loki v3.6.x needs a long `start_period` (120 s) in its healthcheck; and `OTEL_SERVICE_NAME` should be the base name (the code appends `-{SERVER_ID}`).

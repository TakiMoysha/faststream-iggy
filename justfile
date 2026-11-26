set dotenv-load

# ex. just run ./examples/bootstrap.py
run file:
    IGGY_URL="iggy+tcp://takimoysha:abc123@127.0.0.1:8090" \
      uv run {{ file }}
podman-run-iggy:
    podman run --rm --replace -d --name iggy \
      --cap-add=SYS_NICE --security-opt seccomp=unconfined --ulimit memlock=-1:-1 \
      -e IGGY_ROOT_USERNAME=takimoysha -e IGGY_ROOT_PASSWORD=abc123 \
      -e IGGY_HTTP_ENABLED=true -e IGGY_HTTP_ADDRESS=0.0.0.0:3010 \
      -e IGGY_TCP_ENABLED=true -e IGGY_TCP_ADDRESS=0.0.0.0:8090 \
      -e IGGY_NODE_ADVERTISED_ADDRESS=localhost \
      -e PUBLIC_IGGY_API_URL="http://localhost:3010" \
      -p 8090:8090 -p 3010:3010 \
      -v iggy-data:/app/local_data \
      apache/iggy:latest
fulltest:
    IGGY_URL="iggy+tcp://takimoysha:abc123@127.0.0.1:8090" \
      uv run pytest

unittest *ARGS:
    IGGY_URL="iggy+tcp://takimoysha:abc123@127.0.0.1:8090" \
      uv run python -m unittest discover -s tests -v {{ ARGS }}

## Superset

Apache Superset is included for visualizing the Gold Iceberg tables through Trino.

Start it with the stack:

```bash
docker compose up -d --build superset
```

Open Superset at http://localhost:8088 and log in with:

- Username: `admin`
- Password: `admin`

The init container registers a database named `Iceberg` with this Trino URI:

```text
trino://superset@trino:8080/iceberg/gold
```

In Superset, create a dataset from database `Iceberg`, schema `gold`, table
`portfolio_positions`.

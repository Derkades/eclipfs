CREATE TABLE "user" (
  "id" serial PRIMARY KEY,
  "username" text NOT NULL UNIQUE,
  "password" text,
  "write_access" boolean DEFAULT 'False' NOT NULL
);

CREATE TABLE "node" (
  "id" serial PRIMARY KEY,
  -- "address" text,
  "uptime" float NOT NULL DEFAULT 1.0,
  "priority_download" float NOT NULL DEFAULT 1.0,
  "priority_upload" float NOT NULL DEFAULT 1.0,
  "token" text NOT NULL,
  "name" text
  -- "operator" serial,
  -- CONSTRAINT "fk_operator"
  --   FOREIGN KEY("operator")
	--     REFERENCES "user"("id")
);

CREATE TABLE "inode" (
  "id" serial UNIQUE PRIMARY KEY,
  -- "inode" bigint UNIQUE NOT NULL,
  "parent" serial NOT NULL REFERENCES inode(id),
  "is_file" boolean NOT NULL,
  "name" text NOT NULL,
  "ctime" bigint NOT NULL,
  "mtime" bigint NOT NULL
);

INSERT INTO "inode" VALUES (1, 1, 'False', 'IF YOU SEE THIS SOMETHING IS VERY BROKEN', 996616800000, 996616800000);
ALTER SEQUENCE "inode_id_seq" RESTART WITH 2;

-- CREATE TABLE "directory" (
--   "id" serial PRIMARY KEY,
--   "name" TEXT NOT NULL,
--   -- "path" TEXT NOT NULL,
--   "parent" BIGINT REFERENCES "directory"("id"),
--   -- "size_count" BIGINT NOT NULL DEFAULT 0,
--   -- "size_bytes" BIGINT NOT NULL DEFAULT 0,
--   -- "owner" serial NOT NULL REFERENCES "user"("id"),
--   -- "group_read" boolean NOT NULL DEFAULT 'true',
--   -- "group_write" boolean NOT NULL DEFAULT 'false',
--   -- "public_read" boolean NOT NULL DEFAULT 'true',
--   -- "public_write" boolean NOT NULL DEFAULT 'false',
--   -- "replication" int NOT NULL,
--   "delete_time" BIGINT DEFAULT NULL-- NULL if not deleted
-- );

-- CREATE TABLE "file" (
--   "id" serial PRIMARY KEY,
--   "name" text NOT NULL,
--   "directory" serial NOT NULL REFERENCES "directory"("id"),
--   -- "size" bigint,
--   -- "distributed" bool NOT NULL,
--   -- "uploaded" bool NOT NULL DEFAULT 'false',
--   -- "checksum" bytea,
--   -- "chunksize" int NOT NULL,
--   "delete_time" BIGINT -- NULL if not deleted
-- );

-- CREATE TABLE "file_storage" (
--   -- "id" serial PRIMARY KEY,
--   "file" serial NOT NULL UNIQUE REFERENCES "file"("id"),
--   "data" bytea NOT NULL
-- );

CREATE TABLE "chunk" (
  "id" serial PRIMARY KEY,
  "index" int NOT NULL,
  "size" bigint NOT NULL,
  "file" serial NOT NULL REFERENCES "inode"("id") ON DELETE CASCADE,
  "checksum" bytea NOT NULL,
  "token" text NOT NULL UNIQUE,
  UNIQUE("index", "file")
);

CREATE TABLE "chunk_node" (
  "chunk" serial NOT NULL REFERENCES "chunk"("id") ON DELETE CASCADE,
  "node" serial NOT NULL REFERENCES "node"("id") ON DELETE CASCADE
);

-- CREATE TABLE "delete_jobs" (
--   "id" serial PRIMARY KEY,
--   "node" serial NOT NULL REFERENCES "node"("id")
-- );

-- Allow using TABLESAMPLE SYSTEM_ROWS(n)
-- https://www.postgresql.org/docs/current/tsm-system-rows.html
CREATE EXTENSION tsm_system_rows;

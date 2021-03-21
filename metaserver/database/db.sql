CREATE TABLE "user" (
  "id" serial PRIMARY KEY,
  "username" text NOT NULL UNIQUE,
  "password" text NOT NULL,
  "write_access" boolean DEFAULT 'False' NOT NULL
);

CREATE TABLE "node" (
  "id" serial PRIMARY KEY,
  "token" text NOT NULL,
  "location" text NOT NULL,
  "name" text NOT NULL
);

CREATE TABLE "inode" (
  "id" serial UNIQUE PRIMARY KEY,
  "parent" serial NOT NULL REFERENCES inode(id),
  "is_file" boolean NOT NULL,
  "name" text NOT NULL,
  "ctime" bigint NOT NULL,
  "mtime" bigint NOT NULL
);

INSERT INTO "inode" VALUES (1, 1, 'False', 'IF YOU SEE THIS SOMETHING IS VERY BROKEN', 996616800000, 996616800000);
ALTER SEQUENCE "inode_id_seq" RESTART WITH 2;

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

-- Allow using TABLESAMPLE SYSTEM_ROWS(n)
-- https://www.postgresql.org/docs/current/tsm-system-rows.html
CREATE EXTENSION tsm_system_rows;

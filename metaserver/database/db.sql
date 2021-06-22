CREATE TABLE "user" (
  "id" serial PRIMARY KEY,
  "username" text NOT NULL UNIQUE,
  "password" text NOT NULL,
  "write_access" boolean DEFAULT 'False' NOT NULL
);

CREATE INDEX "user_username_idx" ON "user" ("username");

CREATE TABLE "node" (
  "id" serial PRIMARY KEY,
  "token" text NOT NULL,
  "location" text NOT NULL,
  "name" text NOT NULL,
  CHECK(char_length("token") = 32)
);

CREATE INDEX "node_token_idx" ON "node" ("token");

CREATE TABLE "inode" (
  "id" serial UNIQUE PRIMARY KEY,
  "parent" serial NOT NULL REFERENCES inode(id),
  "is_file" boolean NOT NULL,
  "name" text NOT NULL,
  "ctime" bigint NOT NULL,
  "mtime" bigint NOT NULL,
  "chunk_size" int
);

CREATE INDEX "inode_parent_idx" ON "inode" ("parent");
CREATE INDEX "inode_name_idx" ON "inode" ("name");

INSERT INTO "inode" VALUES (1, 1, 'False', 'IF YOU SEE THIS SOMETHING IS VERY BROKEN', 996616800000, 996616800000);
ALTER SEQUENCE "inode_id_seq" RESTART WITH 2;

CREATE TABLE "chunk" (
  "id" serial PRIMARY KEY,
  "file" serial NOT NULL REFERENCES "inode"("id") ON DELETE CASCADE,
  "index" int NOT NULL,
  "size" bigint NOT NULL,
  "checksum" bytea NOT NULL,
  UNIQUE("index", "file")
);

CREATE TABLE "chunk_writing" (
  "id" serial PRIMARY KEY,
  "file" bigint NOT NULL,
  "index" int NOT NULL,
  "size" bigint NOT NULL,
  "checksum" bytea NOT NULL,
  "time" bigint NOT NULL,
  UNIQUE("index", "file")
);

CREATE INDEX "chunk_id_index_idx" ON "chunk" ("id", "index");
CREATE INDEX "chunk_file_idx" ON "chunk" ("file");

CREATE TABLE "chunk_node" (
  "chunk" serial NOT NULL REFERENCES "chunk"("id") ON DELETE CASCADE,
  "node" serial NOT NULL REFERENCES "node"("id") ON DELETE CASCADE,
  UNIQUE("chunk", "node")
);

CREATE INDEX "chunk_node_chunk_node_idx" ON "chunk_node" ("chunk", "node");

CREATE TABLE "meta" (
  "key" text NOT NULL,
  "value_str" text,
  "value_int" bigint
);

INSERT INTO "meta" ("key", "value_int") VALUES ('db_version', 0);

-- Allow using TABLESAMPLE SYSTEM_ROWS(n)
-- https://www.postgresql.org/docs/current/tsm-system-rows.html
CREATE EXTENSION tsm_system_rows;

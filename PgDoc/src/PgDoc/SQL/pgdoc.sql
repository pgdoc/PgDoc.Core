-- Copyright 2016 Flavien Charlon
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--     http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- ======================================================================
-- document: Contains all the documents.
-- ======================================================================

CREATE TABLE document
(
    id      uuid PRIMARY KEY,
    index   bigserial,
    body    jsonb,
    version bytea NOT NULL
);

-- ======================================================================
-- update_documents: Updates a series of documents.
-- ======================================================================

CREATE TYPE document_update AS
(
    id uuid,
    body jsonb,
    version bytea,
    check_only boolean
);

CREATE OR REPLACE FUNCTION update_documents(documents jsonb, version bytea)
RETURNS VOID AS $$ #variable_conflict use_variable
DECLARE
    conflict_id uuid;
    document_updates document_update[];
BEGIN

    -- Parse the input

    document_updates = ARRAY(
      SELECT (
        (json_document ->> 'i')::uuid,
        CASE WHEN json_document ->> 'b' IS NULL THEN NULL ELSE (json_document ->> 'b')::jsonb END,
        decode((json_document ->> 'v')::text, 'hex'),
        (json_document ->> 'c')::boolean)
      FROM jsonb_array_elements(documents) AS json_document);

    -- Insert the new documents

    INSERT INTO document (id, body, version)
    SELECT document_update.id, NULL, E'\\x'
    FROM UNNEST(document_updates) AS document_update
    ON CONFLICT (id) DO NOTHING;

    -- This query returns conflicting rows, the result must be empty
    -- "FOR SHARE" ensures existing documents don't get modified before the UPDATE statement

    WITH document_update AS (
      SELECT document.id, document.version AS old_version, document_update.version AS new_version
      FROM document, UNNEST(document_updates) AS document_update
      WHERE document.id = document_update.id
      FOR SHARE OF document
    )
    SELECT id INTO conflict_id
    FROM document_update
    WHERE old_version <> new_version;

    IF conflict_id IS NOT NULL THEN
      RAISE EXCEPTION 'check_violation' USING HINT = 'update_documents_conflict', DETAIL = conflict_id::text;
    END IF;

    -- Update existing documents

    UPDATE document
    SET body = document_update.body,
        version = version
    FROM UNNEST(document_updates) AS document_update
    WHERE document.id = document_update.id AND NOT document_update.check_only;

END $$ LANGUAGE plpgsql
SECURITY DEFINER;

-- ======================================================================
-- get_documents: Gets a list of documents from their IDs.
-- ======================================================================

CREATE OR REPLACE FUNCTION get_documents(ids uuid[])
RETURNS TABLE (id uuid, body jsonb, version bytea) AS $$
BEGIN

    RETURN QUERY
    SELECT document.id, document.body, document.version
    FROM document, UNNEST(ids) AS document_id
    WHERE document.id = document_id;

END $$ LANGUAGE plpgsql;

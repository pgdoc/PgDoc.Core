CREATE SCHEMA wistap;

CREATE TABLE wistap.object
(
    id      uuid PRIMARY KEY,
    index   bigserial,
    value   jsonb,
    version bytea NOT NULL
);

---------------------------------------------------

---------------------------------------------------
--- Update the value of an existing object

CREATE TYPE wistap.object_update AS
(
    id uuid,
    value jsonb,
    version bytea,
    check_only boolean
);

CREATE OR REPLACE FUNCTION wistap.update_objects(objects jsonb, version bytea)
RETURNS VOID
AS $$ #variable_conflict use_variable
DECLARE
    conflict_id uuid;
    object_updates wistap.object_update[];
BEGIN

    -- Parse the input

    object_updates = ARRAY(
      SELECT (
        (json_object ->> 'i')::uuid,
        CASE WHEN json_object ->> 'v' IS NULL THEN NULL ELSE (json_object ->> 'v')::jsonb END,
        decode((json_object ->> 'ver')::text, 'hex'),
        (json_object ->> 'c')::boolean)
      FROM jsonb_array_elements(objects) AS json_object);

    -- Insert the new objects

    INSERT INTO wistap.object (id, value, version)
    SELECT object_update.id, NULL, E'\\x'
    FROM UNNEST(object_updates) AS object_update
    ON CONFLICT (id) DO NOTHING;

    -- This query returns conflicting rows, the result must be empty
    -- "FOR SHARE" ensures existing objects don't get modified before the UPDATE statement

    WITH object AS (
      SELECT object.id, object.version AS old_version, object_update.version AS new_version
      FROM wistap.object, UNNEST(object_updates) AS object_update
      WHERE object.id = object_update.id
      FOR SHARE OF object
    )
    SELECT id INTO conflict_id
    FROM object
    WHERE old_version <> new_version;

    IF conflict_id IS NOT NULL THEN
      RAISE EXCEPTION 'check_violation' USING HINT = 'update_objects_conflict', DETAIL = conflict_id::text;
    END IF;

    -- Update existing objects

    UPDATE wistap.object
    SET value = object_update.value,
        version = version
    FROM UNNEST(object_updates) AS object_update
    WHERE object.id = object_update.id AND NOT object_update.check_only;

END $$ LANGUAGE plpgsql;

---------------------------------------------------
--- Get a list of objects given their IDs

CREATE OR REPLACE FUNCTION wistap.get_objects(ids uuid[])
RETURNS TABLE (id uuid, value jsonb, version bytea)
AS $$ #variable_conflict use_variable BEGIN

    RETURN QUERY
    SELECT object.id, object.value, object.version
    FROM wistap.object, UNNEST(ids) AS object_id
    WHERE object.id = object_id;

END $$ LANGUAGE plpgsql;

---------------------------------------------------
--- Extract the object type from an object ID

CREATE OR REPLACE FUNCTION wistap.get_object_type(id uuid)
RETURNS smallint
AS $$ DECLARE
    bytes bytea;
BEGIN

    bytes = decode(substring(id::text, 1, 4), 'hex');
    RETURN (get_byte(bytes, 0)::smallint << 8) | get_byte(bytes, 1)::smallint;

END $$ LANGUAGE plpgsql IMMUTABLE;

---------------------------------------------------

---------------------------------------------------
--- Indexes

--CREATE INDEX object_account_type_idx ON wistap.object (account, (wistap.get_object_type(id)))
--WHERE value IS NOT NULL;

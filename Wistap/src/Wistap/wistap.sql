CREATE SCHEMA wistap;

CREATE TABLE wistap.object
(
    id      uuid PRIMARY KEY,
    index   bigserial,
    account bytea NOT NULL,
    payload jsonb,
    version bytea NOT NULL
);

---------------------------------------------------

---------------------------------------------------
--- Update the payload of an existing object

CREATE OR REPLACE FUNCTION wistap.update_objects(account bytea, objects jsonb, version bytea)
RETURNS TABLE (id uuid)
AS $$ #variable_conflict use_variable BEGIN

    -- Parse the input

    CREATE LOCAL TEMP TABLE modified_object
    ON COMMIT DROP AS
    SELECT  (json_object ->> 'k')::uuid as id,
            CASE WHEN json_object ->> 'p' IS NULL THEN NULL ELSE (json_object ->> 'p')::jsonb END as payload,
            decode((json_object ->> 'v')::text, 'hex') as version
    FROM    jsonb_array_elements(objects) as json_object;

    -- This query returns conflicting rows, the result must be empty
    -- "FOR UPDATE" ensures existing objects don't get modified before the UPDATE statement

    RETURN QUERY
    SELECT modified_object.id
    FROM modified_object
    LEFT OUTER JOIN (
      SELECT object.id, object.version
      FROM modified_object, wistap.object
      WHERE object.id = modified_object.id AND object.account = account
      FOR UPDATE OF object NOWAIT
    ) AS existing_object
    ON existing_object.id = modified_object.id
    WHERE modified_object.version <> COALESCE(existing_object.version, E'\\x');

    IF FOUND THEN
      RETURN;
    END IF;

    -- Insert new objects

    INSERT INTO wistap.object (id, account, payload, version)
    SELECT modified_object.id,
           account,
           modified_object.payload,
           version
    FROM modified_object
    WHERE modified_object.version = E'\\x';

    -- Update existing objects

    UPDATE wistap.object
    SET payload = modified_object.payload,
        version = version
    FROM modified_object
    WHERE object.id = modified_object.id AND modified_object.version <> E'\\x';

    DROP TABLE modified_object;

END $$ LANGUAGE plpgsql;

---------------------------------------------------
--- Get a list of objects given their IDs

CREATE OR REPLACE FUNCTION wistap.get_objects(account bytea, ids uuid[])
RETURNS TABLE (id uuid, payload jsonb, version bytea)
AS $$ #variable_conflict use_variable BEGIN

    RETURN QUERY
    SELECT object.id, object.payload, object.version
    FROM wistap.object, UNNEST(ids) AS object_id
    WHERE object.id = object_id AND object.account = account
    FOR SHARE OF object NOWAIT;

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

CREATE INDEX object_account_type_idx ON wistap.object (account, (wistap.get_object_type(id)))
WHERE payload IS NOT NULL;

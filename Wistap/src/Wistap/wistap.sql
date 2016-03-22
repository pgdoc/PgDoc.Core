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
RETURNS VOID
AS $$ #variable_conflict use_variable
DECLARE
    conflict_id uuid;
BEGIN

    -- Parse the input

    CREATE LOCAL TEMP TABLE modified_object
    ON COMMIT DROP AS
    SELECT  (json_object ->> 'i')::uuid as id,
            CASE WHEN json_object ->> 'p' IS NULL THEN NULL ELSE (json_object ->> 'p')::jsonb END as payload,
            decode((json_object ->> 'v')::text, 'hex') as version,
            (json_object ->> 'c')::boolean as check_only
    FROM    jsonb_array_elements(objects) as json_object;

    -- Insert the new objects

    INSERT INTO wistap.object (id, account, payload, version)
    SELECT modified_object.id,
           account,
           NULL,
           E'\\x'
    FROM modified_object
    ON CONFLICT DO NOTHING;

    -- This query returns conflicting rows, the result must be empty
    -- "FOR UPDATE" ensures existing objects don't get modified before the UPDATE statement

    SELECT modified_object.id INTO conflict_id
    FROM modified_object, wistap.object
    WHERE object.id = modified_object.id AND
          (object.version <> modified_object.version OR object.account <> account)
    FOR UPDATE OF object;

    IF conflict_id IS NOT NULL THEN
      RAISE EXCEPTION 'check_violation' USING HINT = conflict_id::text;
    END IF;

    -- Update existing objects

    UPDATE wistap.object
    SET payload = modified_object.payload,
        version = version
    FROM modified_object
    WHERE object.id = modified_object.id AND NOT modified_object.check_only;

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
    WHERE object.id = object_id AND object.account = account;

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

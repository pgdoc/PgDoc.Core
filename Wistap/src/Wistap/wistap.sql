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

CREATE OR REPLACE FUNCTION wistap.update_object(id uuid, account bytea, payload jsonb, version bytea)
RETURNS bytea
AS $$ #variable_conflict use_variable
DECLARE
    result bytea;
BEGIN

    IF version = E'\\x' THEN
        INSERT INTO wistap.object (id, account, payload, version)
        VALUES (
            id,
            account,
            payload,
            substring(decode(md5(coalesce(payload::text, '')), 'hex'), 1, 8))
        ON CONFLICT ON CONSTRAINT object_pkey DO NOTHING
        RETURNING object.version INTO result;
    ELSE
        UPDATE wistap.object
        SET payload = payload,
            version = substring(decode(md5(coalesce(payload::text, '') || encode(object.version, 'hex')), 'hex'), 1, 8)
        WHERE object.id = id AND object.version = version
        RETURNING object.version INTO result;
    END IF;

    RETURN result;

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
    FOR SHARE;

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

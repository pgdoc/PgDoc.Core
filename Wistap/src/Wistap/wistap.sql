CREATE SCHEMA wistap;

CREATE TABLE wistap.object
(
    id              bigserial PRIMARY KEY,
    account_key     bytea NOT NULL,
    type            smallint NOT NULL,
    payload         jsonb NOT NULL,
    version         bytea NOT NULL
);

---------------------------------------------------
--- Create a new object

CREATE OR REPLACE FUNCTION wistap.create_object(account_key bytea, type smallint, payload jsonb)
RETURNS bigint
AS $$ #variable_conflict use_variable
DECLARE
    result bigint;
BEGIN

    INSERT INTO wistap.object (account_key, type, payload, version)
    VALUES (account_key, type, payload, substring(decode(md5(payload::text), 'hex'), 1, 8))
    RETURNING id INTO result;

    RETURN result;

END $$ LANGUAGE plpgsql;

---------------------------------------------------
--- Update the payload of an existing object

CREATE OR REPLACE FUNCTION wistap.update_object(id bigint, payload jsonb, version bytea)
RETURNS bytea
AS $$ #variable_conflict use_variable
DECLARE
    result bytea;
BEGIN

    UPDATE wistap.object
    SET payload = payload,
        version = substring(decode(md5(payload::text), 'hex'), 1, 8)
    WHERE object.id = id AND object.version = version
    RETURNING version INTO result;

    RETURN result;

END $$ LANGUAGE plpgsql;

---------------------------------------------------
--- Get a list of objects given their IDs

CREATE OR REPLACE FUNCTION wistap.get_objects(account_key bytea, ids bigint[])
RETURNS TABLE (id bigint, type smallint, payload jsonb, version bytea)
AS $$ #variable_conflict use_variable BEGIN

    RETURN QUERY
    SELECT object.id, object.type, object.payload, object.version
    FROM wistap.object, UNNEST(ids) AS object_id
    WHERE object.id = object_id AND object.account_key = account_key;

END $$ LANGUAGE plpgsql;

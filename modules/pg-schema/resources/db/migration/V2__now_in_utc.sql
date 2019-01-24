CREATE OR REPLACE FUNCTION ${schema}append_events(expected_version bigint, streams_id varchar,
                                                   events ${schema}event[])
    RETURNS bigint AS $$
DECLARE
    current_version bigint;
    notify_position bigint;
    next_position bigint; -- position of the last event written to the stream - returned
    now timestamp := (SELECT (now() at time zone 'utc'));
    i int;
BEGIN
    IF (SELECT array_length(events, 1) is NULL) THEN
        RAISE EXCEPTION 'no events provided!';
    END IF;

    -- This takes a ROW SHARE lock which will force other transactions which want to UPDATE/DELETE the
    -- same stream_id to either:
    --  * wait until this tx finishes and do their work under isolation level < REPEATABLE READ
    --  * throw an error if isolation level is REPEATABLE READ or SERIALIZABLE
    SELECT position INTO current_version FROM ${schema}streams WHERE id = streams_id FOR UPDATE;
    IF NOT FOUND THEN
        current_version := -1;
    END IF;

    IF expected_version <> -2 AND expected_version <> current_version THEN
        RAISE EXCEPTION 'expected version: % does not match current version: %!', expected_version, current_version;
    END IF;

    IF current_version = -1 THEN
        -- this will fail if someone else created the stream first. That's fine.
        INSERT INTO ${schema}streams (id, position) VALUES (streams_id, 0);
    END IF;

    FOR i IN 1..(SELECT array_length(events, 1)) LOOP
        INSERT INTO ${schema}events (stream_id, timestamp, type, data, metadata) VALUES
                                     (streams_id, now, events[i].type,
                                      events[i].data::${type}, events[i].metadata::${type});
    END LOOP;
    -- we could have other transaction append more events into other streams
    -- so we can't just add the number of events written during this tx.
    UPDATE ${schema}streams AS s SET position =
        (SELECT COALESCE(MAX(position), 0) FROM ${schema}events AS e WHERE e.stream_id = streams_id)
        WHERE s.id = streams_id RETURNING position INTO next_position;

    ------ notification
    -- `new_events_after` position is guaranteed to precede all of the events appended in this call
    IF current_version = -1 THEN
        SELECT min(position) - 1 INTO notify_position FROM ${schema}events WHERE stream_id = streams_id;
    ELSE
        notify_position := current_version;
    END IF;

    PERFORM pg_notify('new_events_after', notify_position::text);

    RETURN next_position;
END;
$$ LANGUAGE plpgsql;

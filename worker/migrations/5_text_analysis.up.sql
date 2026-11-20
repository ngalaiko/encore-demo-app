ALTER TABLE jobs RENAME COLUMN payload TO input_text;
ALTER TABLE jobs ADD COLUMN word_count integer;

CREATE FUNCTION count_words(text) RETURNS integer
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT count(*)::integer
    FROM regexp_matches($1 COLLATE "C",
        U&'[^\0009\000A\000B\000C\000D\0020\0085\00A0\1680\2000-\200A\2028\2029\202F\205F\3000]+', 'g');
$$;

UPDATE jobs SET word_count = count_words(input_text) WHERE status = 'completed';
ALTER TABLE jobs ADD CONSTRAINT jobs_word_count_check
    CHECK (word_count >= 0 AND (status = 'completed') = (word_count IS NOT NULL));

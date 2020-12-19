#The 2 important indexes
CREATE INDEX ts_uid
    USING BTREE
    ON tweet (ts, author);

CREATE INDEX uid_ts
	USING BTREE
    ON tweet (author, ts);

#Not really needed
CREATE INDEX uid
    ON tweet (author);

CREATE INDEX ts
    ON tweet (ts);

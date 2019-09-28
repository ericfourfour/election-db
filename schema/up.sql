CREATE TABLE electoral_districts (
      ed_code INTEGER
    , ed_namee TEXT
    , ed_namef TEXT
    , population INTEGER
    PRIMARY KEY (ed_code)
);

CREATE TABLE parties (
      title TEXT
    , short_name TEXT
    , eligible_dt INTEGER
    , registered_dt INTEGER
    , deregistered_dt INTEGER
    , leader TEXT
    , logo TEXT
    , website TEXT
    , national_headquarters TEXT
    , chief_agent TEXT
    , auditor TEXT
    PRIMARY KEY (title)
);

CREATE TABLE candidates (
	  name TEXT
	, ed_code INTEGER
	, party_title TEXT
	, nomination_dt TEXT
	, cabinet_position TEXT
	, email TEXT
	, phone TEXT
	, photo TEXT
	, donate TEXT
    , volunteer TEXT
    , lawnsign TEXT
	, website TEXT
	, facebook TEXT
	, instagram TEXT
	, twitter TEXT
	, bio TEXT
	, PRIMARY KEY (name)
	, FOREIGN KEY (ed_code) REFERENCES electoral_districts(ed_code)
	, FOREIGN KEY (party_title) REFERENCES parties(title)
);
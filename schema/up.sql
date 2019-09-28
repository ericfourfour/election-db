CREATE TABLE electoral_districts (
      ed_code INTEGER PRIMARY KEY
    , ed_namee TEXT
    , ed_namef TEXT
    , population INTEGER
);

CREATE TABLE parties (
      title TEXT PRIMARY KEY
    , short_name TEXT
    , eligible_dt TEXT
    , registered_dt TEXT
    , deregistered_dt TEXT
    , leader TEXT
    , logo TEXT
    , website TEXT
    , national_headquarters TEXT
    , chief_agent TEXT
    , auditor TEXT
);

CREATE TABLE candidates (
	  ed_code INTEGER
	, party_title TEXT
	, name TEXT
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
	, PRIMARY KEY (ed_code, party_title)
	, FOREIGN KEY (ed_code) REFERENCES electoral_districts(ed_code)
	, FOREIGN KEY (party_title) REFERENCES parties(title)
);
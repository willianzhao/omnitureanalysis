
use stitching;

create external table user_browse_behavior_all_platform_stubtrans (
UserID			STRING,
PurchaseTime	STRING,
PurchaseTicketID	STRING,
TransactionID	STRING,
PurchaseEventID		STRING,
PurchaseEventDesc   STRING,
PurchaseGenreIDOnLevel1	 STRING, 
PurchaseGenreDescOnLevel1 	STRING,
UsMweb_EventIDMatchCount   STRING,
UsMweb_Level1GenreIDMatchCount STRING,
UsMweb_Level2GenreIDMatchCount STRING, 
UsMweb_Level3GenreIDMatchCount STRING,
UsMweb_Level4GenreIDMatchCount STRING,
UsMweb_Level5GenreIDMatchCount STRING,
UsMweb_2a_Flag STRING,
UsMweb_2b_Flag STRING,
iPadApps_EventIDMatchCount STRING,
iPadApps_Level1GenreIDMatchCount STRING,
iPadApps_Level2GenreIDMatchCount STRING,
iPadApps_Level3GenreIDMatchCount STRING,
iPadApps_Level4GenreIDMatchCount STRING,
iPadApps_Level5GenreIDMatchCount STRING,
iPadApps_2a_Flag STRING,
iPadApps_2b_Flag STRING,
MobileApps_EventIDMatchCount STRING,
MobileApps_Level1GenreIDMatchCount STRING,
MobileApps_Level2GenreIDMatchCount STRING,
MobileApps_Level3GenreIDMatchCount STRING,
MobileApps_Level4GenreIDMatchCount STRING,
MobileApps_Level5GenreIDMatchCount STRING,
MobileApps_2a_Flag STRING,
MobileApps_2b_Flag STRING,
Flag_2a STRING,
Flag_2b STRING,
Flag_2c STRING,
OtherFlag STRING,
Top2ndGenre STRING,
GCF STRING,
NewUser STRING
)
PARTITIONED BY (PERIOD STRING)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY '~'
STORED AS TEXTFILE
;

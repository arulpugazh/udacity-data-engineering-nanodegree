DROP TABLE IF EXISTS "TeamsSummary";
DROP TABLE IF EXISTS "ResultsSummary";
DROP TABLE IF EXISTS "MGameCities";
DROP TABLE IF EXISTS "MNCAATourneyDetailedResults";
DROP TABLE IF EXISTS "MNCAATourneySeeds";
DROP TABLE IF EXISTS "MRegularSeasonDetailedResults";
DROP TABLE IF EXISTS "MTeamCoaches";
DROP TABLE IF EXISTS "MSeasons";
DROP TABLE IF EXISTS "MTeams";

CREATE TABLE "MSeasons"
(
    "Season"  DECIMAL NOT NULL primary key,
    "DayZero" VARCHAR NOT NULL,
    "RegionW" VARCHAR NOT NULL,
    "RegionX" VARCHAR NOT NULL,
    "RegionY" VARCHAR NOT NULL,
    "RegionZ" VARCHAR NOT NULL
);

CREATE TABLE "MTeams"
(
    "TeamID"        DECIMAL NOT NULL primary key,
    "TeamName"      VARCHAR NOT NULL,
    "FirstD1Season" DECIMAL NOT NULL,
    "LastD1Season"  DECIMAL NOT NULL
);

CREATE TABLE "MTeamCoaches"
(
    "Season"      DECIMAL NOT NULL references MSeasons (Season),
    "TeamID"      DECIMAL NOT NULL references MTeams (TeamID),
    "FirstDayNum" DECIMAL NOT NULL,
    "LastDayNum"  DECIMAL NOT NULL,
    "CoachName"   VARCHAR NOT NULL,
    primary key (Season, TeamID, CoachName)
);

CREATE TABLE "MGameCities"
(
    "Season"  DECIMAL NOT NULL references MSeasons (Season),
    "DayNum"  DECIMAL NOT NULL,
    "WTeamID" DECIMAL NOT NULL references MTeams (TeamID),
    "LTeamID" DECIMAL NOT NULL references MTeams (TeamID),
    "CRType"  VARCHAR NOT NULL,
    "CityID"  DECIMAL NOT NULL,
    primary key (Season, DayNum, WTeamID, LTeamID, CityID)
);


CREATE TABLE "MNCAATourneyDetailedResults"
(
    "Season"  DECIMAL NOT NULL references MSeasons (Season),
    "DayNum"  DECIMAL NOT NULL,
    "WTeamID" DECIMAL NOT NULL references MTeams (TeamId),
    "WScore"  DECIMAL NOT NULL,
    "LTeamID" DECIMAL NOT NULL references MTeams (TeamId),
    "LScore"  DECIMAL NOT NULL,
    "WLoc"    VARCHAR NOT NULL,
    "NumOT"   DECIMAL NOT NULL,
    "WFGM"    DECIMAL NOT NULL,
    "WFGA"    DECIMAL NOT NULL,
    "WFGM3"   DECIMAL NOT NULL,
    "WFGA3"   DECIMAL NOT NULL,
    "WFTM"    DECIMAL NOT NULL,
    "WFTA"    DECIMAL NOT NULL,
    "WOR"     DECIMAL NOT NULL,
    "WDR"     DECIMAL NOT NULL,
    "WAst"    DECIMAL NOT NULL,
    "WTO"     DECIMAL NOT NULL,
    "WStl"    DECIMAL NOT NULL,
    "WBlk"    DECIMAL NOT NULL,
    "WPF"     DECIMAL NOT NULL,
    "LFGM"    DECIMAL NOT NULL,
    "LFGA"    DECIMAL NOT NULL,
    "LFGM3"   DECIMAL NOT NULL,
    "LFGA3"   DECIMAL NOT NULL,
    "LFTM"    DECIMAL NOT NULL,
    "LFTA"    DECIMAL NOT NULL,
    "LOR"     DECIMAL NOT NULL,
    "LDR"     DECIMAL NOT NULL,
    "LAst"    DECIMAL NOT NULL,
    "LTO"     DECIMAL NOT NULL,
    "LStl"    DECIMAL NOT NULL,
    "LBlk"    DECIMAL NOT NULL,
    "LPF"     DECIMAL NOT NULL,
    primary key (Season, DayNum, WTeamID, LTeamID)
);

CREATE TABLE "MNCAATourneySeeds"
(
    "Season" DECIMAL NOT NULL references MSeasons (Season),
    "Seed"   VARCHAR NOT NULL,
    "TeamID" DECIMAL NOT NULL references MTeams (TeamID),
    primary key (Season, TeamID, Seed)
);


CREATE TABLE "MRegularSeasonDetailedResults"
(
    "Season"  DECIMAL NOT NULL references MSeasons (Season),
    "DayNum"  DECIMAL NOT NULL,
    "WTeamID" DECIMAL NOT NULL references MTeams (TeamID),
    "WScore"  DECIMAL NOT NULL,
    "LTeamID" DECIMAL NOT NULL references MTeams (TeamID),
    "LScore"  DECIMAL NOT NULL,
    "WLoc"    VARCHAR NOT NULL,
    "NumOT"   DECIMAL NOT NULL,
    "WFGM"    DECIMAL NOT NULL,
    "WFGA"    DECIMAL NOT NULL,
    "WFGM3"   DECIMAL NOT NULL,
    "WFGA3"   DECIMAL NOT NULL,
    "WFTM"    DECIMAL NOT NULL,
    "WFTA"    DECIMAL NOT NULL,
    "WOR"     DECIMAL NOT NULL,
    "WDR"     DECIMAL NOT NULL,
    "WAst"    DECIMAL NOT NULL,
    "WTO"     DECIMAL NOT NULL,
    "WStl"    DECIMAL NOT NULL,
    "WBlk"    DECIMAL NOT NULL,
    "WPF"     DECIMAL NOT NULL,
    "LFGM"    DECIMAL NOT NULL,
    "LFGA"    DECIMAL NOT NULL,
    "LFGM3"   DECIMAL NOT NULL,
    "LFGA3"   DECIMAL NOT NULL,
    "LFTM"    DECIMAL NOT NULL,
    "LFTA"    DECIMAL NOT NULL,
    "LOR"     DECIMAL NOT NULL,
    "LDR"     DECIMAL NOT NULL,
    "LAst"    DECIMAL NOT NULL,
    "LTO"     DECIMAL NOT NULL,
    "LStl"    DECIMAL NOT NULL,
    "LBlk"    DECIMAL NOT NULL,
    "LPF"     DECIMAL NOT NULL,
    primary key (Season, DayNum, WTeamID, LTeamID)
);

CREATE TABLE "ResultsSummary"
(
    "Season"    DECIMAL NOT NULL references MSeasons (Season),
    "DayNum"    DECIMAL NOT NULL,
    "TeamID"    DECIMAL NOT NULL references MTeams (TeamID),
    "OppTeamID" DECIMAL NOT NULL references MTeams (TeamID),
    "Result"    BOOLEAN NOT NULL,
    "Score"     DECIMAL NOT NULL,
    "Loc"       VARCHAR NOT NULL,
    "NumOT"     DECIMAL NOT NULL,
    "FGM"       DECIMAL NOT NULL,
    "FGA"       DECIMAL NOT NULL,
    "FGM3"      DECIMAL NOT NULL,
    "FGA3"      DECIMAL NOT NULL,
    "FTM"       DECIMAL NOT NULL,
    "FTA"       DECIMAL NOT NULL,
    "OR"        DECIMAL NOT NULL,
    "DR"        DECIMAL NOT NULL,
    "Ast"       DECIMAL NOT NULL,
    "TO"        DECIMAL NOT NULL,
    "Stl"       DECIMAL NOT NULL,
    "Blk"       DECIMAL NOT NULL,
    "PF"        DECIMAL NOT NULL,
    "CoachName" VARCHAR NOT NULL,
    "CityID"    DECIMAL NOT NULL,
    "Seed"      VARCHAR NOT NULL,
    primary key (Season, DayNum, TeamID),
    foreign key (Season, TeamID, Seed) references MNCAATourneySeeds (Season, TeamID, Seed),
    foreign key (Season, TeamID, CoachName) references MTeamCoaches (Season, TeamID, CoachName),
    foreign key (Season, DayNum, TeamID, OppTeamID, CityID) references MGameCities (Season, DayNum, WTeamID, LTeamID, CityID)
);
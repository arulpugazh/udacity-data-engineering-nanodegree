class MarchMadnessQueries:
    win_tourney = '''INSERT INTO resultssummary(season, daynum, teamid, oppteamid, result,
                           score, loc, numot, fgm, fga, fgm3, fga3, ftm, fta,
                           "or", dr, ast, "to", stl, blk, pf, seed, coachname, cityid)
    SELECT res.season,
           res.daynum,
           res.wteamid AS TeamId,
           res.lteamid AS OppTeamId,
           1           AS result,
           wscore      AS Score,
           wloc        AS Loc,
           NumOT       AS NumOT,
           wFGM        AS FGM,
           wFGA        AS FGA,
           wFGM3       AS FGM3,
           wFGA3       AS FGA3,
           wFTM        AS FTM,
           wFTA        AS FTA,
           wOR         AS "OR",
           wDR         AS DR,
           wAst        AS Ast,
           wTO         AS "TO",
           wStl        AS Stl,
           wBlk        AS Blk,
           wPF         AS PF,
           seeds.seed  AS Seed,
           c.coachname AS coachname,
           g.cityid    AS cityid
    FROM mncaatourneydetailedresults AS res
             JOIN mncaatourneyseeds seeds ON res.season = seeds.season AND res.wteamid = seeds.teamid
             JOIN mteamcoaches c on res.season = c.season and res.wteamid = c.teamid
             JOIN mgamecities g on c.season = g.season and res.daynum = g.daynum and
                                   res.wteamid = g.wteamid and res.lteamid = g.lteamid;'''

    win_regular = '''INSERT INTO resultssummary(season, daynum, teamid, oppteamid, result,
                           score, loc, numot, fgm, fga, fgm3, fga3, ftm, fta,
                           "or", dr, ast, "to", stl, blk, pf, seed, coachname, cityid)
    SELECT res.season,
           res.daynum,
           res.wteamid AS TeamId,
           res.lteamid AS OppTeamId,
           1           AS result,
           wscore      AS Score,
           wloc        AS Loc,
           NumOT       AS NumOT,
           wFGM        AS FGM,
           wFGA        AS FGA,
           wFGM3       AS FGM3,
           wFGA3       AS FGA3,
           wFTM        AS FTM,
           wFTA        AS FTA,
           wOR         AS "OR",
           wDR         AS DR,
           wAst        AS Ast,
           wTO         AS "TO",
           wStl        AS Stl,
           wBlk        AS Blk,
           wPF         AS PF,
           seeds.seed  AS Seed,
           c.coachname AS coachname,
           g.cityid    AS cityid
    FROM mregularseasondetailedresults AS res
             JOIN mncaatourneyseeds seeds ON res.season = seeds.season AND res.wteamid = seeds.teamid
             JOIN mteamcoaches c on res.season = c.season and res.wteamid = c.teamid
             JOIN mgamecities g on c.season = g.season and res.daynum = g.daynum and
                                   res.wteamid = g.wteamid and res.lteamid = g.lteamid;'''

    loss_tourney = '''INSERT INTO resultssummary(season, daynum, teamid, oppteamid, result,
                           score, loc, numot, fgm, fga, fgm3, fga3, ftm, fta,
                           "or", dr, ast, "to", stl, blk, pf, seed, coachname, cityid)
    SELECT res.season,
           res.daynum,
           res.lteamid AS TeamId,
           res.wteamid AS OppTeamId,
           0           AS result,
           lscore      AS Score,
           CASE
               when wloc = 'Home' then 'Away'
               when wloc = 'Away' then 'Home'
               else 'Neutral'
               END
                       AS Loc,
           NumOT       AS NumOT,
           lFGM        AS FGM,
           lFGA        AS FGA,
           lFGM3       AS FGM3,
           lFGA3       AS FGA3,
           lFTM        AS FTM,
           lFTA        AS FTA,
           lOR         AS "OR",
           lDR         AS DR,
           lAst        AS Ast,
           lTO         AS "TO",
           lStl        AS Stl,
           lBlk        AS Blk,
           lPF         AS PF,
           seeds.seed  AS Seed,
           c.coachname AS coachname,
           g.cityid    AS cityid
    FROM mncaatourneydetailedresults AS res
             JOIN mncaatourneyseeds seeds ON res.season = seeds.season AND res.lteamid = seeds.teamid
             JOIN mteamcoaches c on res.season = c.season and res.lteamid = c.teamid
             JOIN mgamecities g on c.season = g.season and res.daynum = g.daynum and
                                   res.wteamid = g.wteamid and res.lteamid = g.lteamid;'''

    loss_regular = '''INSERT INTO resultssummary(season, daynum, teamid, oppteamid, result,
                           score, loc, numot, fgm, fga, fgm3, fga3, ftm, fta,
                           "or", dr, ast, "to", stl, blk, pf, seed, coachname, cityid)
    SELECT res.season,
           res.daynum,
           res.lteamid AS TeamId,
           res.wteamid AS OppTeamId,
           0           AS result,
           lscore      AS Score,
           CASE
               when wloc = 'Home' then 'Away'
               when wloc = 'Away' then 'Home'
               else 'Neutral'
               END
                       AS Loc,
           NumOT       AS NumOT,
           lFGM        AS FGM,
           lFGA        AS FGA,
           lFGM3       AS FGM3,
           lFGA3       AS FGA3,
           lFTM        AS FTM,
           lFTA        AS FTA,
           lOR         AS "OR",
           lDR         AS DR,
           lAst        AS Ast,
           lTO         AS "TO",
           lStl        AS Stl,
           lBlk        AS Blk,
           lPF         AS PF,
           seeds.seed  AS Seed,
           c.coachname AS coachname,
           g.cityid    AS cityid
    FROM mregularseasondetailedresults AS res
             JOIN mncaatourneyseeds seeds ON res.season = seeds.season AND res.lteamid = seeds.teamid
             JOIN mteamcoaches c on res.season = c.season and res.lteamid = c.teamid
             JOIN mgamecities g
                  on c.season = g.season and res.daynum = g.daynum and
                     res.wteamid = g.wteamid and res.lteamid = g.lteamid;'''
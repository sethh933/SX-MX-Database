SELECT
    COALESCE(rm.year, sh.year, sl.year, sq.year) AS year,
    COALESCE(rm.round, sh.round, sl.round, sq.round) AS round,
    COALESCE(rm.trackname, sh.trackname, sl.trackname, sq.trackname) AS trackname,
    COALESCE(sm.classID, sh.classID, sl.classID, sq.classID) AS classID,
    COALESCE(sm.coastid, sh.coastid, sl.coastid, sq.coastid) AS coastid,
    COALESCE(r.riderid, sh.riderid, sl.riderid, sq.riderid) AS riderid,
    COALESCE(r.FullName, sh.FullName, sl.FullName, sq.FullName) AS FullName,
    sm.laps_led,
    CASE WHEN sm.raceid IS NOT NULL THEN sm.Result END AS mains_Result,
    sm.Points,
    SUM(COALESCE(sm.Points, 0)) OVER (PARTITION BY COALESCE(rm.year, sh.year, sl.year, sq.year), COALESCE(sm.classID, sh.classID, sl.classID, sq.classID), COALESCE(r.riderid, sh.riderid, sl.riderid, sq.riderid) ORDER BY COALESCE(rm.round, sh.round, sl.round, sq.round)) AS TotalPoints,
    sh.Result AS heats_Result,
    sl.Result AS lcqs_Result,
    sq.Result AS quals_Result
FROM
    Race_Table rm
FULL OUTER JOIN
    SX_MAINS sm ON rm.raceid = sm.raceid
JOIN
    Rider_List r ON sm.riderid = r.riderid
FULL OUTER JOIN (
    SELECT
        rmh.year,
        rmh.round,
        rmh.trackname,
        sh.classID,
        sh.coastid,
        sh.riderid,
        sh.FullName,
        sh.Result
    FROM
        Race_Table rmh
    JOIN
        SX_HEATS sh ON rmh.raceid = sh.raceid
) sh ON COALESCE(rm.year, sh.year) = sh.year
    AND COALESCE(rm.round, sh.round) = sh.round
    AND COALESCE(rm.trackname, sh.trackname) = sh.trackname
    AND COALESCE(sm.classID, sh.classID) = sh.classID
    AND COALESCE(r.riderid, sh.riderid) = sh.riderid
FULL OUTER JOIN (
    SELECT
        rmlcqs.year,
        rmlcqs.round,
        rmlcqs.trackname,
        sl.classID,
        sl.coastid,
        sl.riderid,
        sl.FullName,
        sl.Result
    FROM
        Race_Table rmlcqs
    JOIN
        SX_LCQS sl ON rmlcqs.raceid = sl.raceid
) sl ON COALESCE(rm.year, sh.year, sl.year) = sl.year
    AND COALESCE(rm.round, sh.round, sl.round) = sl.round
    AND COALESCE(rm.trackname, sh.trackname, sl.trackname) = sl.trackname
    AND COALESCE(sm.classID, sh.classID, sl.classID) = sl.classID
    AND COALESCE(r.riderid, sh.riderid, sl.riderid) = sl.riderid
FULL OUTER JOIN (
    SELECT
        rmquals.year,
        rmquals.round,
        rmquals.trackname,
        sq.classID,
        sq.coastid,
        sq.riderid,
        sq.FullName,
        sq.Result
    FROM
        Race_Table rmquals
    JOIN
        SX_QUAL sq ON rmquals.raceid = sq.raceid
) sq ON COALESCE(rm.year, sh.year, sl.year, sq.year) = sq.year
    AND COALESCE(rm.round, sh.round, sl.round, sq.round) = sq.round
    AND COALESCE(rm.trackname, sh.trackname, sl.trackname, sq.trackname) = sq.trackname
    AND COALESCE(sm.classID, sh.classID, sl.classID, sq.classID) = sq.classID
    AND COALESCE(r.riderid, sh.riderid, sl.riderid, sq.riderid) = sq.riderid;

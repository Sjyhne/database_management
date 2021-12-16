/*Find all deaths per country in dwh*/
SELECT L.country, sum(F.total_killed)
FROM dwh.fact F, dwh.location_dim L
WHERE F.country = L.country
GROUP BY L.country;

/*Find all deaths per country in odb*/
SELECT C.country, sum(E.total_killed)
FROM odb.country C, odb.event_info E, odb.provstate P, odb.city Ci
WHERE C.country = P.country and P.provstate = Ci.provstate and Ci.city = E.city
GROUP BY C.country;
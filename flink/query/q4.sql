SELECT
        b.device_id, a.strategy, a.site, a.pos_id, count(b.device_id), count(a.payload)
FROM
        click a
JOIN
        dau b
ON
        a.session_id = b.session_id AND a.rowtime BETWEEN b.rowtime - INTERVAL '1' second AND b.rowtime + INTERVAL '1' second
GROUP BY
        b.device_id, a.strategy, a.site, a.pos_id, TUMBLE(a.rowtime, INTERVAL '1' HOUR)

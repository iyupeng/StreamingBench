SELECT session_id, max(payload)
FROM
    click a
GROUP BY
    a.session_id, TUMBLE(a.rowtime, INTERVAL '1' HOUR)
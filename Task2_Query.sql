SELECT sum(CrmRevenue),
       count(*)
FROM crm_transaction t1
JOIN web_events t2
       ON t1.UserId = t2.UserId
       AND CrmDateTime BETWEEN dateadd(mi, -5, DateTime) AND dateadd(mi, 5, DateTime)
GROUP BY EventParam[0],
         TrafficSource ->> 'utm_source',
         TrafficSource ->> 'utm_campaign',
         cast(CrmDateTime AS date)

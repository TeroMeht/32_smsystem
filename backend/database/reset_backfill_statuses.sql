UPDATE backfill_status
   SET daily_last_run    = NULL,
       intraday_last_run = NULL
 WHERE daily_last_run::date    = CURRENT_DATE
    OR intraday_last_run::date = CURRENT_DATE;
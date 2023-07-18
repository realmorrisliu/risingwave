-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q19_no_output AS
SELECT auction, bidder, price, channel, url, date_time, extra 
FROM (SELECT *, 
             ROW_NUMBER() OVER (
                 PARTITION BY auction 
                 ORDER BY price DESC
             ) AS rank_number 
      FROM bid)
WHERE rank_number <= 10
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');

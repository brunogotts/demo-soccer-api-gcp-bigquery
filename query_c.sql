WITH match_referee_total_card AS (
  SELECT
  match_referee,
  ARRAY_LENGTH(cards) AS total_cards_match
FROM `football-analysis-demo.football_api.events`
WHERE match_referee != ""
)
SELECT
  match_referee,
  SUM(total_cards_match) AS total_cards
FROM match_referee_total_card
GROUP BY 1
ORDER BY 2 DESC, 1 ASC
LIMIT 5